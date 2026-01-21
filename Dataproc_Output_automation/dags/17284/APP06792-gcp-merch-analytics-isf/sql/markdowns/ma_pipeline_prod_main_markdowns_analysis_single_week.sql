/*
Name:                markdowns_analysis_single_week
APPID-Name:          APP09527
Purpose:             Markdowns and Clearance analysis and reporting. 
                     Insert data into table t2dl_das_in_season_management_reporting.markdowns_analysis_single_week
Variable(s):         {{params.environment_schema}} t2dl_das_in_season_management_reporting 
                     {{params.env_suffix}} dev or prod as appropriate
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
CREATE TEMPORARY TABLE IF NOT EXISTS snap_date AS
SELECT MAX(day_date) AS snapshot_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE week_end_day_date < CURRENT_DATE('PST8PDT');
/*
* COPY NAP TABLE CONTENTS
* 
* - Explicitly select columns to protect against potential future changes
* - Select only the latest week in the table; in the future the table will have history, but TBD if that will become part of this pipeline or a different one.
* 
*/
--,dual_exposure_ind
--,warehouse_inventory
--,warehouse_inventory_dollars
--,warehouse_on_order_units_qty
--,clearance_markdown_percent_off
--,future_clearance_markdown_percent_off
--,regular_price_percent_off
--,trunc(f.compare_at_percent_off)*1.00/100 as compare_at_percent_off   -- convert from 51.99 to 0.51 
CREATE TEMPORARY TABLE IF NOT EXISTS cds AS
SELECT snapshot_date,
 rms_style_num,
 color_num,
 channel_country,
 channel_brand,
 selling_channel,
 first_receipt_date,
 last_receipt_date,
 available_to_sell,
 first_sales_date,
 weeks_sales,
 total_sales_amt_1wk,
 total_sales_units_1wk,
 sell_thru_1wk,
 total_sales_amt_2wks,
 total_sales_units_2wks,
 sell_thru_2wks,
 total_sales_amt_4wks,
 total_sales_units_4wks,
 sell_thru_4wks,
 total_sales_amt_6mons,
 total_sales_units_6mons,
 sell_thru_6mons,
 sell_thru_since_last_markdown,
 div_num,
 div_desc,
 grp_num,
 grp_desc,
 dept_num,
 dept_desc,
 class_num,
 class_desc,
 sbclass_num,
 sbclass_desc,
 style_group_num,
 supp_part_num,
 style_desc,
 prmy_supp_num,
 vendor_name,
 vendor_label_name,
 npg_ind,
 msrp_amt,
 anniversary_item_ind,
 is_online_purchasable,
 online_purchasable_eff_begin_tmstp,
 online_purchasable_eff_end_tmstp,
 weighted_average_cost,
 promotion_ind,
 total_inv_qty,
 total_inv_dollars,
 reserve_stock_inventory,
 reserve_stock_inventory_dollars,
 pack_and_hold_inventory,
 pack_and_hold_inventory_dollars,
 on_order_units_qty,
 reserve_stock_on_order_units_qty,
 pack_and_hold_on_order_units_qty,
 first_clearance_markdown_date,
 last_clearance_markdown_date,
 clearance_markdown_version,
 clearance_markdown_state,
 future_clearance_markdown_date,
 future_clearance_markdown_price,
 regular_price_amt,
 ownership_price_amt,
 base_retail_drop_ship_amt,
 current_price_amt,
 current_rack_retail,
 first_rack_date,
 rack_ind,
 supp_color,
 drop_ship_eligible_ind,
 return_disposition_code,
 fp_replenishment_eligible_ind,
 op_replenishment_eligible_ind,
 selling_status_code,
 price_variance_ind
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.clearance_markdown_insights_by_week_fact AS f
WHERE LOWER(channel_country) = LOWER('US')
 AND snapshot_date = (SELECT snapshot_date
   FROM snap_date);
-- no stats recommended for collection
/*
* ADD FIELDS RELATED TO RECORD ID, KEY DIMENSIONS, MERCH HIERARCHY, PRODUCT ATTRIBUTES, PRODUCT STATUS, COST, PRICE, MARKDOWN INFO
* - reorder columns into topical sections for easier understanding of code
* - add more columns for reporting and analysis
* 
*/
/*
      * Record ID and Key Dimensions
      */
-- Data Science & Analytics removes leading zeroes from color for their CC field
-- Creating this fields so we can more easily join to their data in the future
/*
      * Merch Hierarchy
      */
/*
      * Product Attributes
      */
/*
      * Product Status
      */
/*
      * Cost
      * Weighted Average Cost is defined as the cost at the SKU + Loc level. Any higher grain will be Average Unit Cost. 
      * Value in CDS table is up-leveled from SKU + Loc so should be called Average Unit Cost instead.
      * Value in CDS table retains four decimal places (same as WAC) but users of this data think of cost with two decimal places.
      * 
      * This cost is used later to calculate inventory value at cost, so rounding from four decimal places to two will introduce some variation.
      * Therefore, we will round cost at a later step after calculating inventory value.
      */
/*
      * Price Amounts, Price Bands
      */
-- next highest dollar fields will be used to join to price band table in later query
/*
       * Markdown Info
       */
/*
       * Remaining CDS fields to be handled in later queries
       */
CREATE TEMPORARY TABLE IF NOT EXISTS add_product_info AS
SELECT snapshot_date,
 channel_country,
 channel_brand,
  CASE
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM')
  THEN 'N'
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
  THEN 'R'
  ELSE NULL
  END AS banner_code,
 selling_channel,
   rms_style_num || '-' || color_num AS cc,
      CASE
      WHEN LOWER(channel_brand) = LOWER('NORDSTROM')
      THEN 'N'
      WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
      THEN 'R'
      ELSE NULL
      END || '-' || selling_channel || '-' || (rms_style_num || '-' || color_num) AS banner_channel_cc,
   channel_country || '-' || (CASE
        WHEN LOWER(channel_brand) = LOWER('NORDSTROM')
        THEN 'N'
        WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
        THEN 'R'
        ELSE NULL
        END || '-' || selling_channel || '-' || (rms_style_num || '-' || color_num)) AS country_banner_channel_cc,
   TRIM(rms_style_num) || '-' || TRIM(FORMAT('%6d', CAST(CASE
      WHEN COALESCE(color_num, FORMAT('%4d', 0)) = ''
      THEN '0'
      ELSE COALESCE(color_num, FORMAT('%4d', 0))
      END AS SMALLINT))) AS cc_dsci,
 div_num,
   TRIM(FORMAT('%11d', div_num)) || ', ' || div_desc AS div_label,
 grp_num AS subdiv_num,
   TRIM(FORMAT('%11d', grp_num)) || ', ' || grp_desc AS subdiv_label,
 dept_num,
   TRIM(FORMAT('%11d', dept_num)) || ', ' || dept_desc AS dept_label,
 class_num,
   TRIM(FORMAT('%11d', class_num)) || ', ' || class_desc AS class_label,
 sbclass_num AS subclass_num,
   TRIM(FORMAT('%11d', sbclass_num)) || ', ' || sbclass_desc AS subclass_label,
 style_group_num,
 rms_style_num,
 color_num,
 supp_part_num AS vpn,
 supp_color,
 style_desc,
 prmy_supp_num,
 vendor_name,
 vendor_label_name,
 npg_ind,
 promotion_ind,
 anniversary_item_ind,
  CASE
  WHEN LOWER(CASE
     WHEN LOWER(channel_brand) = LOWER('NORDSTROM')
     THEN 'N'
     WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
     THEN 'R'
     ELSE NULL
     END) = LOWER('N')
  THEN fp_replenishment_eligible_ind
  WHEN LOWER(CASE
     WHEN LOWER(channel_brand) = LOWER('NORDSTROM')
     THEN 'N'
     WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
     THEN 'R'
     ELSE NULL
     END) = LOWER('R')
  THEN op_replenishment_eligible_ind
  ELSE NULL
  END AS replenishment_eligible_ind,
 drop_ship_eligible_ind,
 return_disposition_code,
 selling_status_code AS selling_status_code_legacy,
 first_rack_date,
 rack_ind,
 is_online_purchasable AS online_purchasable_ind,
 CAST(online_purchasable_eff_begin_tmstp AS DATE) AS online_purchasable_eff_begin_date,
 CAST(online_purchasable_eff_end_tmstp AS DATE) AS online_purchasable_eff_end_date,
 weighted_average_cost AS average_unit_cost,
 regular_price_amt AS price_base,
 ownership_price_amt AS price_ownership,
 COALESCE(future_clearance_markdown_price, ownership_price_amt) AS price_ownership_future,
 current_price_amt AS price_selling,
 base_retail_drop_ship_amt AS price_base_drop_ship,
 msrp_amt AS price_compare_at,
 current_rack_retail AS price_current_rack,
 price_variance_ind AS price_variance_within_cc_ind,
 CEIL(regular_price_amt) AS price_base_next_highest_dollar,
 CEIL(ownership_price_amt) AS price_ownership_next_highest_dollar,
 CEIL(current_price_amt) AS price_selling_next_highest_dollar,
 CEIL(COALESCE(future_clearance_markdown_price, ownership_price_amt)) AS price_ownership_future_next_highest_dollar,
 first_clearance_markdown_date,
 last_clearance_markdown_date,
 future_clearance_markdown_date,
 clearance_markdown_version,
 clearance_markdown_state,
  CASE
  WHEN future_clearance_markdown_price IS NOT NULL
  THEN 'Y'
  ELSE 'N'
  END AS future_markdown_ind,
 CAST(TRUNC(DATE_DIFF(snapshot_date, last_clearance_markdown_date, DAY) / 7) AS INT64) AS weeks_since_last_markdown,
 first_receipt_date,
 last_receipt_date,
 available_to_sell,
 first_sales_date,
 weeks_sales,
 total_sales_amt_1wk,
 total_sales_units_1wk,
 sell_thru_1wk,
 total_sales_amt_2wks,
 total_sales_units_2wks,
 sell_thru_2wks,
 total_sales_amt_4wks,
 total_sales_units_4wks,
 sell_thru_4wks,
 total_sales_amt_6mons,
 total_sales_units_6mons,
 sell_thru_6mons,
 sell_thru_since_last_markdown,
 total_inv_qty,
 total_inv_dollars,
 reserve_stock_inventory,
 reserve_stock_inventory_dollars,
 pack_and_hold_inventory,
 pack_and_hold_inventory_dollars,
 on_order_units_qty,
 reserve_stock_on_order_units_qty,
 pack_and_hold_on_order_units_qty
FROM cds;
-- no stats recommended for collection
/*
* ADD FIELDS RELATED TO PERCENT OFF 
*/
-- fields from prior query
/*
      * Percent Off
      */
/*
       * Percent Off Bands
       */
/*
       * Remaining CDS fields to be handled in later queries
       */
CREATE TEMPORARY TABLE IF NOT EXISTS add_percent_off AS
SELECT snapshot_date,
 channel_country,
 channel_brand,
 banner_code,
 selling_channel,
 cc,
 banner_channel_cc,
 country_banner_channel_cc,
 cc_dsci,
 div_num,
 div_label,
 subdiv_num,
 subdiv_label,
 dept_num,
 dept_label,
 class_num,
 class_label,
 subclass_num,
 subclass_label,
 style_group_num,
 rms_style_num,
 color_num,
 vpn,
 supp_color,
 style_desc,
 prmy_supp_num,
 vendor_name,
 vendor_label_name,
 npg_ind,
 promotion_ind,
 anniversary_item_ind,
 replenishment_eligible_ind,
 drop_ship_eligible_ind,
 return_disposition_code,
 selling_status_code_legacy,
 first_rack_date,
 rack_ind,
 online_purchasable_ind,
 online_purchasable_eff_begin_date,
 online_purchasable_eff_end_date,
 average_unit_cost,
 price_base,
 price_ownership,
 price_ownership_future,
 price_selling,
 price_base_drop_ship,
 price_compare_at,
 price_current_rack,
 price_variance_within_cc_ind,
 price_base_next_highest_dollar,
 price_ownership_next_highest_dollar,
 price_selling_next_highest_dollar,
 price_ownership_future_next_highest_dollar,
 first_clearance_markdown_date,
 last_clearance_markdown_date,
 future_clearance_markdown_date,
 clearance_markdown_version,
 clearance_markdown_state,
 future_markdown_ind,
 weeks_since_last_markdown,
  CASE
  WHEN price_base = 0
  THEN 0
  ELSE CAST(trunc((price_base - price_ownership) / price_base * 100) AS BIGNUMERIC) / 100
  END AS pct_off_ownership_vs_base,
  CASE
  WHEN price_base = 0
  THEN 0
  ELSE CAST(trunc((price_base - price_selling) / price_base * 100) AS BIGNUMERIC) / 100
  END AS pct_off_selling_vs_base,
  CASE
  WHEN price_base = 0
  THEN 0
  ELSE CAST(trunc((price_base - price_ownership_future) / price_base * 100) AS BIGNUMERIC) / 100
  END AS pct_off_ownership_future_vs_base,
  CASE
  WHEN price_compare_at = 0
  THEN 0
  ELSE CAST(trunc((price_compare_at - price_ownership) / price_compare_at * 100) AS BIGNUMERIC) / 100
  END AS pct_off_ownership_vs_compare_at,
  CASE
  WHEN price_compare_at = 0
  THEN 0
  ELSE CAST(trunc((price_compare_at - price_ownership_future) / price_compare_at * 100) AS BIGNUMERIC) / 100
  END AS pct_off_ownership_future_vs_compare_at,
  CASE
  WHEN CASE
   WHEN price_base = 0
   THEN 0
   ELSE CAST(trunc((price_base - price_ownership) / price_base * 100) AS BIGNUMERIC) / 100
   END IS NULL
  THEN NULL
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_ownership) / price_base * 100) AS BIGNUMERIC) / 100
    END < 0
  THEN '< 0%'
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_ownership) / price_base * 100) AS BIGNUMERIC) / 100
    END = 0
  THEN '0%'
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_ownership) / price_base * 100) AS BIGNUMERIC) / 100
    END <= 0.10
  THEN '01-10%'
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_ownership) / price_base * 100) AS BIGNUMERIC) / 100
    END <= 0.20
  THEN '11-20%'
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_ownership) / price_base * 100) AS BIGNUMERIC) / 100
    END <= 0.30
  THEN '21-30%'
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_ownership) / price_base * 100) AS BIGNUMERIC) / 100
    END <= 0.40
  THEN '31-40%'
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_ownership) / price_base * 100) AS BIGNUMERIC) / 100
    END <= 0.50
  THEN '41-50%'
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_ownership) / price_base * 100) AS BIGNUMERIC) / 100
    END <= 0.60
  THEN '51-60%'
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_ownership) / price_base * 100) AS BIGNUMERIC) / 100
    END <= 0.70
  THEN '61-70%'
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_ownership) / price_base * 100) AS BIGNUMERIC) / 100
    END <= 0.80
  THEN '71-80%'
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_ownership) / price_base * 100) AS BIGNUMERIC) / 100
    END <= 0.90
  THEN '81-90%'
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_ownership) / price_base * 100) AS BIGNUMERIC) / 100
    END <= 1
  THEN '91-100%'
  ELSE 'UNDEFINED'
  END AS pct_off_band_ownership_vs_base,
  CASE
  WHEN CASE
   WHEN price_base = 0
   THEN 0
   ELSE CAST(trunc((price_base - price_selling) / price_base * 100) AS BIGNUMERIC) / 100
   END IS NULL
  THEN NULL
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_selling) / price_base * 100) AS BIGNUMERIC) / 100
    END < 0
  THEN '< 0%'
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_selling) / price_base * 100) AS BIGNUMERIC) / 100
    END = 0
  THEN '0%'
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_selling) / price_base * 100) AS BIGNUMERIC) / 100
    END <= 0.10
  THEN '01-10%'
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_selling) / price_base * 100) AS BIGNUMERIC) / 100
    END <= 0.20
  THEN '11-20%'
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_selling) / price_base * 100) AS BIGNUMERIC) / 100
    END <= 0.30
  THEN '21-30%'
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_selling) / price_base * 100) AS BIGNUMERIC) / 100
    END <= 0.40
  THEN '31-40%'
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_selling) / price_base * 100) AS BIGNUMERIC) / 100
    END <= 0.50
  THEN '41-50%'
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_selling) / price_base * 100) AS BIGNUMERIC) / 100
    END <= 0.60
  THEN '51-60%'
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_selling) / price_base * 100) AS BIGNUMERIC) / 100
    END <= 0.70
  THEN '61-70%'
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_selling) / price_base * 100) AS BIGNUMERIC) / 100
    END <= 0.80
  THEN '71-80%'
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_selling) / price_base * 100) AS BIGNUMERIC) / 100
    END <= 0.90
  THEN '81-90%'
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_selling) / price_base * 100) AS BIGNUMERIC) / 100
    END <= 1
  THEN '91-100%'
  ELSE 'UNDEFINED'
  END AS pct_off_band_selling_vs_base,
  CASE
  WHEN CASE
   WHEN price_base = 0
   THEN 0
   ELSE CAST(trunc((price_base - price_ownership_future) / price_base * 100) AS BIGNUMERIC) / 100
   END IS NULL
  THEN NULL
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_ownership_future) / price_base * 100) AS BIGNUMERIC) / 100
    END < 0
  THEN '< 0%'
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_ownership_future) / price_base * 100) AS BIGNUMERIC) / 100
    END = 0
  THEN '0%'
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_ownership_future) / price_base * 100) AS BIGNUMERIC) / 100
    END <= 0.10
  THEN '01-10%'
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_ownership_future) / price_base * 100) AS BIGNUMERIC) / 100
    END <= 0.20
  THEN '11-20%'
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_ownership_future) / price_base * 100) AS BIGNUMERIC) / 100
    END <= 0.30
  THEN '21-30%'
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_ownership_future) / price_base * 100) AS BIGNUMERIC) / 100
    END <= 0.40
  THEN '31-40%'
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_ownership_future) / price_base * 100) AS BIGNUMERIC) / 100
    END <= 0.50
  THEN '41-50%'
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_ownership_future) / price_base * 100) AS BIGNUMERIC) / 100
    END <= 0.60
  THEN '51-60%'
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_ownership_future) / price_base * 100) AS BIGNUMERIC) / 100
    END <= 0.70
  THEN '61-70%'
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_ownership_future) / price_base * 100) AS BIGNUMERIC) / 100
    END <= 0.80
  THEN '71-80%'
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_ownership_future) / price_base * 100) AS BIGNUMERIC) / 100
    END <= 0.90
  THEN '81-90%'
  WHEN CASE
    WHEN price_base = 0
    THEN 0
    ELSE CAST(trunc((price_base - price_ownership_future) / price_base * 100) AS BIGNUMERIC) / 100
    END <= 1
  THEN '91-100%'
  ELSE 'UNDEFINED'
  END AS pct_off_band_ownership_future_vs_base,
  CASE
  WHEN CASE
   WHEN price_compare_at = 0
   THEN 0
   ELSE CAST(trunc((price_compare_at - price_ownership) / price_compare_at * 100) AS BIGNUMERIC) / 100
   END IS NULL
  THEN NULL
  WHEN CASE
    WHEN price_compare_at = 0
    THEN 0
    ELSE CAST(trunc((price_compare_at - price_ownership) / price_compare_at * 100) AS BIGNUMERIC) / 100
    END < 0
  THEN '< 0%'
  WHEN CASE
    WHEN price_compare_at = 0
    THEN 0
    ELSE CAST(trunc((price_compare_at - price_ownership) / price_compare_at * 100) AS BIGNUMERIC) / 100
    END = 0
  THEN '0%'
  WHEN CASE
    WHEN price_compare_at = 0
    THEN 0
    ELSE CAST(trunc((price_compare_at - price_ownership) / price_compare_at * 100) AS BIGNUMERIC) / 100
    END <= 0.10
  THEN '01-10%'
  WHEN CASE
    WHEN price_compare_at = 0
    THEN 0
    ELSE CAST(trunc((price_compare_at - price_ownership) / price_compare_at * 100) AS BIGNUMERIC) / 100
    END <= 0.20
  THEN '11-20%'
  WHEN CASE
    WHEN price_compare_at = 0
    THEN 0
    ELSE CAST(trunc((price_compare_at - price_ownership) / price_compare_at * 100) AS BIGNUMERIC) / 100
    END <= 0.30
  THEN '21-30%'
  WHEN CASE
    WHEN price_compare_at = 0
    THEN 0
    ELSE CAST(trunc((price_compare_at - price_ownership) / price_compare_at * 100) AS BIGNUMERIC) / 100
    END <= 0.40
  THEN '31-40%'
  WHEN CASE
    WHEN price_compare_at = 0
    THEN 0
    ELSE CAST(trunc((price_compare_at - price_ownership) / price_compare_at * 100) AS BIGNUMERIC) / 100
    END <= 0.50
  THEN '41-50%'
  WHEN CASE
    WHEN price_compare_at = 0
    THEN 0
    ELSE CAST(trunc((price_compare_at - price_ownership) / price_compare_at * 100) AS BIGNUMERIC) / 100
    END <= 0.60
  THEN '51-60%'
  WHEN CASE
    WHEN price_compare_at = 0
    THEN 0
    ELSE CAST(trunc((price_compare_at - price_ownership) / price_compare_at * 100) AS BIGNUMERIC) / 100
    END <= 0.70
  THEN '61-70%'
  WHEN CASE
    WHEN price_compare_at = 0
    THEN 0
    ELSE CAST(trunc((price_compare_at - price_ownership) / price_compare_at * 100) AS BIGNUMERIC) / 100
    END <= 0.80
  THEN '71-80%'
  WHEN CASE
    WHEN price_compare_at = 0
    THEN 0
    ELSE CAST(trunc((price_compare_at - price_ownership) / price_compare_at * 100) AS BIGNUMERIC) / 100
    END <= 0.90
  THEN '81-90%'
  WHEN CASE
    WHEN price_compare_at = 0
    THEN 0
    ELSE CAST(trunc((price_compare_at - price_ownership) / price_compare_at * 100) AS BIGNUMERIC) / 100
    END <= 1
  THEN '91-100%'
  ELSE 'UNDEFINED'
  END AS pct_off_band_ownership_vs_compare_at,
  CASE
  WHEN CASE
   WHEN price_compare_at = 0
   THEN 0
   ELSE CAST(trunc((price_compare_at - price_ownership_future) / price_compare_at * 100) AS BIGNUMERIC) / 100
   END IS NULL
  THEN NULL
  WHEN CASE
    WHEN price_compare_at = 0
    THEN 0
    ELSE CAST(trunc((price_compare_at - price_ownership_future) / price_compare_at * 100) AS BIGNUMERIC) / 100
    END < 0
  THEN '< 0%'
  WHEN CASE
    WHEN price_compare_at = 0
    THEN 0
    ELSE CAST(trunc((price_compare_at - price_ownership_future) / price_compare_at * 100) AS BIGNUMERIC) / 100
    END = 0
  THEN '0%'
  WHEN CASE
    WHEN price_compare_at = 0
    THEN 0
    ELSE CAST(trunc((price_compare_at - price_ownership_future) / price_compare_at * 100) AS BIGNUMERIC) / 100
    END <= 0.10
  THEN '01-10%'
  WHEN CASE
    WHEN price_compare_at = 0
    THEN 0
    ELSE CAST(trunc((price_compare_at - price_ownership_future) / price_compare_at * 100) AS BIGNUMERIC) / 100
    END <= 0.20
  THEN '11-20%'
  WHEN CASE
    WHEN price_compare_at = 0
    THEN 0
    ELSE CAST(trunc((price_compare_at - price_ownership_future) / price_compare_at * 100) AS BIGNUMERIC) / 100
    END <= 0.30
  THEN '21-30%'
  WHEN CASE
    WHEN price_compare_at = 0
    THEN 0
    ELSE CAST(trunc((price_compare_at - price_ownership_future) / price_compare_at * 100) AS BIGNUMERIC) / 100
    END <= 0.40
  THEN '31-40%'
  WHEN CASE
    WHEN price_compare_at = 0
    THEN 0
    ELSE CAST(trunc((price_compare_at - price_ownership_future) / price_compare_at * 100) AS BIGNUMERIC) / 100
    END <= 0.50
  THEN '41-50%'
  WHEN CASE
    WHEN price_compare_at = 0
    THEN 0
    ELSE CAST(trunc((price_compare_at - price_ownership_future) / price_compare_at * 100) AS BIGNUMERIC) / 100
    END <= 0.60
  THEN '51-60%'
  WHEN CASE
    WHEN price_compare_at = 0
    THEN 0
    ELSE CAST(trunc((price_compare_at - price_ownership_future) / price_compare_at * 100) AS BIGNUMERIC) / 100
    END <= 0.70
  THEN '61-70%'
  WHEN CASE
    WHEN price_compare_at = 0
    THEN 0
    ELSE CAST(trunc((price_compare_at - price_ownership_future) / price_compare_at * 100) AS BIGNUMERIC) / 100
    END <= 0.80
  THEN '71-80%'
  WHEN CASE
    WHEN price_compare_at = 0
    THEN 0
    ELSE CAST(trunc((price_compare_at - price_ownership_future) / price_compare_at * 100) AS BIGNUMERIC) / 100
    END <= 0.90
  THEN '81-90%'
  WHEN CASE
    WHEN price_compare_at = 0
    THEN 0
    ELSE CAST(trunc((price_compare_at - price_ownership_future) / price_compare_at * 100) AS BIGNUMERIC) / 100
    END <= 1
  THEN '91-100%'
  ELSE 'UNDEFINED'
  END AS pct_off_band_ownership_future_vs_compare_at,
 first_receipt_date,
 last_receipt_date,
 available_to_sell,
 first_sales_date,
 weeks_sales,
 total_sales_amt_1wk,
 total_sales_units_1wk,
 sell_thru_1wk,
 total_sales_amt_2wks,
 total_sales_units_2wks,
 sell_thru_2wks,
 total_sales_amt_4wks,
 total_sales_units_4wks,
 sell_thru_4wks,
 total_sales_amt_6mons,
 total_sales_units_6mons,
 sell_thru_6mons,
 sell_thru_since_last_markdown,
 total_inv_qty,
 total_inv_dollars,
 reserve_stock_inventory,
 reserve_stock_inventory_dollars,
 pack_and_hold_inventory,
 pack_and_hold_inventory_dollars,
 on_order_units_qty,
 reserve_stock_on_order_units_qty,
 pack_and_hold_on_order_units_qty
FROM add_product_info;
-- no stats recommended for collection
/*
* ADD FIELDS FOR PRICE TYPE AND PRICE SIGNALS (.97, .01) 
*/
-- fields from prior query
/*
       * Price Type, Price Signals
       * 
       * Signal fields not created for selling price because signals are based on ownership price, not selling price
       */
-- Price Type
-- ownership > reg should not happen, but assume it's Reg 
-- ownership > reg could happen if there is a future-dated change to reg price
-- selling > ownership should not happen, but assume it's non-promo
/*
       * Price Signals
       * 
       * .97 indicator based on price ending
       * .01 indicator based on entire price (not price ending)
       * NULL otherwise 
       */
/*
       * Remaining CDS fields to be handled in later queries
       */
CREATE TEMPORARY TABLE IF NOT EXISTS add_price_type AS
SELECT snapshot_date,
 channel_country,
 channel_brand,
 banner_code,
 selling_channel,
 cc,
 banner_channel_cc,
 country_banner_channel_cc,
 cc_dsci,
 div_num,
 div_label,
 subdiv_num,
 subdiv_label,
 dept_num,
 dept_label,
 class_num,
 class_label,
 subclass_num,
 subclass_label,
 style_group_num,
 rms_style_num,
 color_num,
 vpn,
 supp_color,
 style_desc,
 prmy_supp_num,
 vendor_name,
 vendor_label_name,
 npg_ind,
 promotion_ind,
 anniversary_item_ind,
 replenishment_eligible_ind,
 drop_ship_eligible_ind,
 return_disposition_code,
 selling_status_code_legacy,
 first_rack_date,
 rack_ind,
 online_purchasable_ind,
 online_purchasable_eff_begin_date,
 online_purchasable_eff_end_date,
 average_unit_cost,
 price_base,
 price_ownership,
 price_ownership_future,
 price_selling,
 price_base_drop_ship,
 price_compare_at,
 price_current_rack,
 price_variance_within_cc_ind,
 price_base_next_highest_dollar,
 price_ownership_next_highest_dollar,
 price_selling_next_highest_dollar,
 price_ownership_future_next_highest_dollar,
 first_clearance_markdown_date,
 last_clearance_markdown_date,
 future_clearance_markdown_date,
 clearance_markdown_version,
 clearance_markdown_state,
 future_markdown_ind,
 weeks_since_last_markdown,
 pct_off_ownership_vs_base,
 pct_off_selling_vs_base,
 pct_off_ownership_future_vs_base,
 pct_off_ownership_vs_compare_at,
 pct_off_ownership_future_vs_compare_at,
 pct_off_band_ownership_vs_base,
 pct_off_band_selling_vs_base,
 pct_off_band_ownership_future_vs_base,
 pct_off_band_ownership_vs_compare_at,
 pct_off_band_ownership_future_vs_compare_at,
  CASE
  WHEN price_ownership >= price_base
  THEN 'R'
  WHEN price_ownership < price_base
  THEN 'C'
  ELSE 'U'
  END AS price_type_code_ownership,
  CASE
  WHEN price_ownership_future >= price_base
  THEN 'R'
  WHEN price_ownership_future < price_base
  THEN 'C'
  ELSE 'U'
  END AS price_type_code_ownership_future,
  CASE
  WHEN price_selling < price_ownership
  THEN 'P'
  ELSE CASE
   WHEN price_ownership >= price_base
   THEN 'R'
   WHEN price_ownership < price_base
   THEN 'C'
   ELSE 'U'
   END
  END AS price_type_code_selling,
  CASE
  WHEN LOWER(CASE
     WHEN price_ownership >= price_base
     THEN 'R'
     WHEN price_ownership < price_base
     THEN 'C'
     ELSE 'U'
     END) = LOWER('R')
  THEN 'REGULAR'
  WHEN LOWER(CASE
     WHEN price_ownership >= price_base
     THEN 'R'
     WHEN price_ownership < price_base
     THEN 'C'
     ELSE 'U'
     END) = LOWER('C')
  THEN 'CLEARANCE'
  WHEN LOWER(CASE
     WHEN price_ownership >= price_base
     THEN 'R'
     WHEN price_ownership < price_base
     THEN 'C'
     ELSE 'U'
     END) = LOWER('U')
  THEN 'UNKNOWN'
  ELSE NULL
  END AS price_type_ownership,
  CASE
  WHEN LOWER(CASE
     WHEN price_ownership_future >= price_base
     THEN 'R'
     WHEN price_ownership_future < price_base
     THEN 'C'
     ELSE 'U'
     END) = LOWER('R')
  THEN 'REGULAR'
  WHEN LOWER(CASE
     WHEN price_ownership_future >= price_base
     THEN 'R'
     WHEN price_ownership_future < price_base
     THEN 'C'
     ELSE 'U'
     END) = LOWER('C')
  THEN 'CLEARANCE'
  WHEN LOWER(CASE
     WHEN price_ownership_future >= price_base
     THEN 'R'
     WHEN price_ownership_future < price_base
     THEN 'C'
     ELSE 'U'
     END) = LOWER('U')
  THEN 'UNKNOWN'
  ELSE NULL
  END AS price_type_ownership_future,
  CASE
  WHEN LOWER(CASE
     WHEN price_selling < price_ownership
     THEN 'P'
     ELSE CASE
      WHEN price_ownership >= price_base
      THEN 'R'
      WHEN price_ownership < price_base
      THEN 'C'
      ELSE 'U'
      END
     END) = LOWER('R')
  THEN 'REGULAR'
  WHEN LOWER(CASE
     WHEN price_selling < price_ownership
     THEN 'P'
     ELSE CASE
      WHEN price_ownership >= price_base
      THEN 'R'
      WHEN price_ownership < price_base
      THEN 'C'
      ELSE 'U'
      END
     END) = LOWER('C')
  THEN 'CLEARANCE'
  WHEN LOWER(CASE
     WHEN price_selling < price_ownership
     THEN 'P'
     ELSE CASE
      WHEN price_ownership >= price_base
      THEN 'R'
      WHEN price_ownership < price_base
      THEN 'C'
      ELSE 'U'
      END
     END) = LOWER('U')
  THEN 'UNKNOWN'
  WHEN LOWER(CASE
     WHEN price_selling < price_ownership
     THEN 'P'
     ELSE CASE
      WHEN price_ownership >= price_base
      THEN 'R'
      WHEN price_ownership < price_base
      THEN 'C'
      ELSE 'U'
      END
     END) = LOWER('P')
  THEN 'PROMOTION'
  ELSE NULL
  END AS price_type_selling,
  CASE
  WHEN MOD(IFNULL(price_ownership, 0), 1) = 0.97
  THEN '97'
  WHEN price_ownership = 0.01
  THEN '01'
  ELSE NULL
  END AS price_signal,
  CASE
  WHEN MOD(IFNULL(price_ownership_future, 0), 1) = 0.97
  THEN '97'
  WHEN price_ownership_future = 0.01
  THEN '01'
  ELSE NULL
  END AS price_signal_future,
 first_receipt_date,
 last_receipt_date,
 available_to_sell,
 first_sales_date,
 weeks_sales,
 total_sales_amt_1wk,
 total_sales_units_1wk,
 sell_thru_1wk,
 total_sales_amt_2wks,
 total_sales_units_2wks,
 sell_thru_2wks,
 total_sales_amt_4wks,
 total_sales_units_4wks,
 sell_thru_4wks,
 total_sales_amt_6mons,
 total_sales_units_6mons,
 sell_thru_6mons,
 sell_thru_since_last_markdown,
 total_inv_qty,
 total_inv_dollars,
 reserve_stock_inventory,
 reserve_stock_inventory_dollars,
 pack_and_hold_inventory,
 pack_and_hold_inventory_dollars,
 on_order_units_qty,
 reserve_stock_on_order_units_qty,
 pack_and_hold_on_order_units_qty
FROM add_percent_off;
-- no stats recommended for collection
/*
* ADD FIELDS RELATED TO LIFECYCLE PHASE 
*/
-- fields from prior query
/*
       * Markdown Type, Lifecycle Phase 
       */
-- markdown type
-- future markdown exists and item is currently clearance
-- future markdown exists and item is currently reg price
-- create lifcycle phase names using same values as lifecycle phase data
-- if markdown type is unknown then lifecycle phase will also be unknown
/*
       * Remaining CDS fields to be handled in later queries
       */
CREATE TEMPORARY TABLE IF NOT EXISTS add_lifecycle_phase AS
SELECT snapshot_date,
 channel_country,
 channel_brand,
 banner_code,
 selling_channel,
 cc,
 banner_channel_cc,
 country_banner_channel_cc,
 cc_dsci,
 div_num,
 div_label,
 subdiv_num,
 subdiv_label,
 dept_num,
 dept_label,
 class_num,
 class_label,
 subclass_num,
 subclass_label,
 style_group_num,
 rms_style_num,
 color_num,
 vpn,
 supp_color,
 style_desc,
 prmy_supp_num,
 vendor_name,
 vendor_label_name,
 npg_ind,
 promotion_ind,
 anniversary_item_ind,
 replenishment_eligible_ind,
 drop_ship_eligible_ind,
 return_disposition_code,
 selling_status_code_legacy,
 first_rack_date,
 rack_ind,
 online_purchasable_ind,
 online_purchasable_eff_begin_date,
 online_purchasable_eff_end_date,
 average_unit_cost,
 price_base,
 price_ownership,
 price_ownership_future,
 price_selling,
 price_base_drop_ship,
 price_compare_at,
 price_current_rack,
 price_variance_within_cc_ind,
 price_base_next_highest_dollar,
 price_ownership_next_highest_dollar,
 price_selling_next_highest_dollar,
 price_ownership_future_next_highest_dollar,
 first_clearance_markdown_date,
 last_clearance_markdown_date,
 future_clearance_markdown_date,
 clearance_markdown_version,
 clearance_markdown_state,
 future_markdown_ind,
 weeks_since_last_markdown,
 pct_off_ownership_vs_base,
 pct_off_selling_vs_base,
 pct_off_ownership_future_vs_base,
 pct_off_ownership_vs_compare_at,
 pct_off_ownership_future_vs_compare_at,
 pct_off_band_ownership_vs_base,
 pct_off_band_selling_vs_base,
 pct_off_band_ownership_future_vs_base,
 pct_off_band_ownership_vs_compare_at,
 pct_off_band_ownership_future_vs_compare_at,
 price_type_code_ownership,
 price_type_code_ownership_future,
 price_type_code_selling,
 price_type_ownership,
 price_type_ownership_future,
 price_type_selling,
 price_signal,
 price_signal_future,
  CASE
  WHEN LOWER(price_type_code_ownership) = LOWER('C')
  THEN CASE
   WHEN LOWER(price_signal) = LOWER('01')
   THEN 'LAST CHANCE'
   WHEN LOWER(banner_code) = LOWER('N') AND LOWER(price_signal) = LOWER('97')
   THEN 'RACKING'
   WHEN clearance_markdown_version > 1
   THEN 'REMARK'
   WHEN clearance_markdown_version = 1
   THEN 'FIRST MARK'
   ELSE 'UNKNOWN'
   END
  WHEN LOWER(price_type_code_ownership) = LOWER('R')
  THEN 'REG PRICE'
  ELSE 'UNKNOWN'
  END AS markdown_type_ownership,
  CASE
  WHEN LOWER(price_type_code_ownership_future) = LOWER('C')
  THEN CASE
   WHEN LOWER(price_signal_future) = LOWER('01')
   THEN 'LAST CHANCE'
   WHEN LOWER(banner_code) = LOWER('N') AND LOWER(price_signal_future) = LOWER('97')
   THEN 'RACKING'
   WHEN LOWER(future_markdown_ind) = LOWER('Y') AND clearance_markdown_version > 0
   THEN 'REMARK'
   WHEN LOWER(future_markdown_ind) = LOWER('Y') AND clearance_markdown_version = 0
   THEN 'FIRST MARK'
   ELSE CASE
    WHEN LOWER(price_type_code_ownership) = LOWER('C')
    THEN CASE
     WHEN LOWER(price_signal) = LOWER('01')
     THEN 'LAST CHANCE'
     WHEN LOWER(banner_code) = LOWER('N') AND LOWER(price_signal) = LOWER('97')
     THEN 'RACKING'
     WHEN clearance_markdown_version > 1
     THEN 'REMARK'
     WHEN clearance_markdown_version = 1
     THEN 'FIRST MARK'
     ELSE 'UNKNOWN'
     END
    WHEN LOWER(price_type_code_ownership) = LOWER('R')
    THEN 'REG PRICE'
    ELSE 'UNKNOWN'
    END
   END
  WHEN LOWER(price_type_code_ownership_future) = LOWER('R')
  THEN 'REG PRICE'
  ELSE 'UNKNOWN'
  END AS markdown_type_ownership_future,
 COALESCE(CASE
    WHEN LOWER(banner_code) = LOWER('N')
    THEN CASE
     WHEN LOWER(CASE
        WHEN LOWER(price_type_code_ownership) = LOWER('C')
        THEN CASE
         WHEN LOWER(price_signal) = LOWER('01')
         THEN 'LAST CHANCE'
         WHEN LOWER(banner_code) = LOWER('N') AND LOWER(price_signal) = LOWER('97')
         THEN 'RACKING'
         WHEN clearance_markdown_version > 1
         THEN 'REMARK'
         WHEN clearance_markdown_version = 1
         THEN 'FIRST MARK'
         ELSE 'UNKNOWN'
         END
        WHEN LOWER(price_type_code_ownership) = LOWER('R')
        THEN 'REG PRICE'
        ELSE 'UNKNOWN'
        END) = LOWER('REG PRICE')
     THEN '1. NORD '
     WHEN LOWER(CASE
        WHEN LOWER(price_type_code_ownership) = LOWER('C')
        THEN CASE
         WHEN LOWER(price_signal) = LOWER('01')
         THEN 'LAST CHANCE'
         WHEN LOWER(banner_code) = LOWER('N') AND LOWER(price_signal) = LOWER('97')
         THEN 'RACKING'
         WHEN clearance_markdown_version > 1
         THEN 'REMARK'
         WHEN clearance_markdown_version = 1
         THEN 'FIRST MARK'
         ELSE 'UNKNOWN'
         END
        WHEN LOWER(price_type_code_ownership) = LOWER('R')
        THEN 'REG PRICE'
        ELSE 'UNKNOWN'
        END) = LOWER('FIRST MARK')
     THEN '2. NORD '
     WHEN LOWER(CASE
        WHEN LOWER(price_type_code_ownership) = LOWER('C')
        THEN CASE
         WHEN LOWER(price_signal) = LOWER('01')
         THEN 'LAST CHANCE'
         WHEN LOWER(banner_code) = LOWER('N') AND LOWER(price_signal) = LOWER('97')
         THEN 'RACKING'
         WHEN clearance_markdown_version > 1
         THEN 'REMARK'
         WHEN clearance_markdown_version = 1
         THEN 'FIRST MARK'
         ELSE 'UNKNOWN'
         END
        WHEN LOWER(price_type_code_ownership) = LOWER('R')
        THEN 'REG PRICE'
        ELSE 'UNKNOWN'
        END) = LOWER('REMARK')
     THEN '3. NORD '
     WHEN LOWER(CASE
        WHEN LOWER(price_type_code_ownership) = LOWER('C')
        THEN CASE
         WHEN LOWER(price_signal) = LOWER('01')
         THEN 'LAST CHANCE'
         WHEN LOWER(banner_code) = LOWER('N') AND LOWER(price_signal) = LOWER('97')
         THEN 'RACKING'
         WHEN clearance_markdown_version > 1
         THEN 'REMARK'
         WHEN clearance_markdown_version = 1
         THEN 'FIRST MARK'
         ELSE 'UNKNOWN'
         END
        WHEN LOWER(price_type_code_ownership) = LOWER('R')
        THEN 'REG PRICE'
        ELSE 'UNKNOWN'
        END) = LOWER('RACKING')
     THEN '4. NORD '
     WHEN LOWER(CASE
        WHEN LOWER(price_type_code_ownership) = LOWER('C')
        THEN CASE
         WHEN LOWER(price_signal) = LOWER('01')
         THEN 'LAST CHANCE'
         WHEN LOWER(banner_code) = LOWER('N') AND LOWER(price_signal) = LOWER('97')
         THEN 'RACKING'
         WHEN clearance_markdown_version > 1
         THEN 'REMARK'
         WHEN clearance_markdown_version = 1
         THEN 'FIRST MARK'
         ELSE 'UNKNOWN'
         END
        WHEN LOWER(price_type_code_ownership) = LOWER('R')
        THEN 'REG PRICE'
        ELSE 'UNKNOWN'
        END) = LOWER('LAST CHANCE')
     THEN '8. '
     ELSE NULL
     END
    WHEN LOWER(banner_code) = LOWER('R')
    THEN CASE
     WHEN LOWER(CASE
        WHEN LOWER(price_type_code_ownership) = LOWER('C')
        THEN CASE
         WHEN LOWER(price_signal) = LOWER('01')
         THEN 'LAST CHANCE'
         WHEN LOWER(banner_code) = LOWER('N') AND LOWER(price_signal) = LOWER('97')
         THEN 'RACKING'
         WHEN clearance_markdown_version > 1
         THEN 'REMARK'
         WHEN clearance_markdown_version = 1
         THEN 'FIRST MARK'
         ELSE 'UNKNOWN'
         END
        WHEN LOWER(price_type_code_ownership) = LOWER('R')
        THEN 'REG PRICE'
        ELSE 'UNKNOWN'
        END) = LOWER('REG PRICE')
     THEN '5. RACK '
     WHEN LOWER(CASE
        WHEN LOWER(price_type_code_ownership) = LOWER('C')
        THEN CASE
         WHEN LOWER(price_signal) = LOWER('01')
         THEN 'LAST CHANCE'
         WHEN LOWER(banner_code) = LOWER('N') AND LOWER(price_signal) = LOWER('97')
         THEN 'RACKING'
         WHEN clearance_markdown_version > 1
         THEN 'REMARK'
         WHEN clearance_markdown_version = 1
         THEN 'FIRST MARK'
         ELSE 'UNKNOWN'
         END
        WHEN LOWER(price_type_code_ownership) = LOWER('R')
        THEN 'REG PRICE'
        ELSE 'UNKNOWN'
        END) = LOWER('FIRST MARK')
     THEN '6. RACK '
     WHEN LOWER(CASE
        WHEN LOWER(price_type_code_ownership) = LOWER('C')
        THEN CASE
         WHEN LOWER(price_signal) = LOWER('01')
         THEN 'LAST CHANCE'
         WHEN LOWER(banner_code) = LOWER('N') AND LOWER(price_signal) = LOWER('97')
         THEN 'RACKING'
         WHEN clearance_markdown_version > 1
         THEN 'REMARK'
         WHEN clearance_markdown_version = 1
         THEN 'FIRST MARK'
         ELSE 'UNKNOWN'
         END
        WHEN LOWER(price_type_code_ownership) = LOWER('R')
        THEN 'REG PRICE'
        ELSE 'UNKNOWN'
        END) = LOWER('REMARK')
     THEN '7. RACK '
     WHEN LOWER(CASE
        WHEN LOWER(price_type_code_ownership) = LOWER('C')
        THEN CASE
         WHEN LOWER(price_signal) = LOWER('01')
         THEN 'LAST CHANCE'
         WHEN LOWER(banner_code) = LOWER('N') AND LOWER(price_signal) = LOWER('97')
         THEN 'RACKING'
         WHEN clearance_markdown_version > 1
         THEN 'REMARK'
         WHEN clearance_markdown_version = 1
         THEN 'FIRST MARK'
         ELSE 'UNKNOWN'
         END
        WHEN LOWER(price_type_code_ownership) = LOWER('R')
        THEN 'REG PRICE'
        ELSE 'UNKNOWN'
        END) = LOWER('LAST CHANCE')
     THEN '8. '
     ELSE NULL
     END
    ELSE NULL
    END || CASE
    WHEN LOWER(price_type_code_ownership) = LOWER('C')
    THEN CASE
     WHEN LOWER(price_signal) = LOWER('01')
     THEN 'LAST CHANCE'
     WHEN LOWER(banner_code) = LOWER('N') AND LOWER(price_signal) = LOWER('97')
     THEN 'RACKING'
     WHEN clearance_markdown_version > 1
     THEN 'REMARK'
     WHEN clearance_markdown_version = 1
     THEN 'FIRST MARK'
     ELSE 'UNKNOWN'
     END
    WHEN LOWER(price_type_code_ownership) = LOWER('R')
    THEN 'REG PRICE'
    ELSE 'UNKNOWN'
    END, 'UNKNOWN') AS lifecycle_phase_name_ownership,
 COALESCE(CASE
    WHEN LOWER(banner_code) = LOWER('N')
    THEN CASE
     WHEN LOWER(CASE
        WHEN LOWER(price_type_code_ownership_future) = LOWER('C')
        THEN CASE
         WHEN LOWER(price_signal_future) = LOWER('01')
         THEN 'LAST CHANCE'
         WHEN LOWER(banner_code) = LOWER('N') AND LOWER(price_signal_future) = LOWER('97')
         THEN 'RACKING'
         WHEN LOWER(future_markdown_ind) = LOWER('Y') AND clearance_markdown_version > 0
         THEN 'REMARK'
         WHEN LOWER(future_markdown_ind) = LOWER('Y') AND clearance_markdown_version = 0
         THEN 'FIRST MARK'
         ELSE CASE
          WHEN LOWER(price_type_code_ownership) = LOWER('C')
          THEN CASE
           WHEN LOWER(price_signal) = LOWER('01')
           THEN 'LAST CHANCE'
           WHEN LOWER(banner_code) = LOWER('N') AND LOWER(price_signal) = LOWER('97')
           THEN 'RACKING'
           WHEN clearance_markdown_version > 1
           THEN 'REMARK'
           WHEN clearance_markdown_version = 1
           THEN 'FIRST MARK'
           ELSE 'UNKNOWN'
           END
          WHEN LOWER(price_type_code_ownership) = LOWER('R')
          THEN 'REG PRICE'
          ELSE 'UNKNOWN'
          END
         END
        WHEN LOWER(price_type_code_ownership_future) = LOWER('R')
        THEN 'REG PRICE'
        ELSE 'UNKNOWN'
        END) = LOWER('REG PRICE')
     THEN '1. NORD '
     WHEN LOWER(CASE
        WHEN LOWER(price_type_code_ownership_future) = LOWER('C')
        THEN CASE
         WHEN LOWER(price_signal_future) = LOWER('01')
         THEN 'LAST CHANCE'
         WHEN LOWER(banner_code) = LOWER('N') AND LOWER(price_signal_future) = LOWER('97')
         THEN 'RACKING'
         WHEN LOWER(future_markdown_ind) = LOWER('Y') AND clearance_markdown_version > 0
         THEN 'REMARK'
         WHEN LOWER(future_markdown_ind) = LOWER('Y') AND clearance_markdown_version = 0
         THEN 'FIRST MARK'
         ELSE CASE
          WHEN LOWER(price_type_code_ownership) = LOWER('C')
          THEN CASE
           WHEN LOWER(price_signal) = LOWER('01')
           THEN 'LAST CHANCE'
           WHEN LOWER(banner_code) = LOWER('N') AND LOWER(price_signal) = LOWER('97')
           THEN 'RACKING'
           WHEN clearance_markdown_version > 1
           THEN 'REMARK'
           WHEN clearance_markdown_version = 1
           THEN 'FIRST MARK'
           ELSE 'UNKNOWN'
           END
          WHEN LOWER(price_type_code_ownership) = LOWER('R')
          THEN 'REG PRICE'
          ELSE 'UNKNOWN'
          END
         END
        WHEN LOWER(price_type_code_ownership_future) = LOWER('R')
        THEN 'REG PRICE'
        ELSE 'UNKNOWN'
        END) = LOWER('FIRST MARK')
     THEN '2. NORD '
     WHEN LOWER(CASE
        WHEN LOWER(price_type_code_ownership_future) = LOWER('C')
        THEN CASE
         WHEN LOWER(price_signal_future) = LOWER('01')
         THEN 'LAST CHANCE'
         WHEN LOWER(banner_code) = LOWER('N') AND LOWER(price_signal_future) = LOWER('97')
         THEN 'RACKING'
         WHEN LOWER(future_markdown_ind) = LOWER('Y') AND clearance_markdown_version > 0
         THEN 'REMARK'
         WHEN LOWER(future_markdown_ind) = LOWER('Y') AND clearance_markdown_version = 0
         THEN 'FIRST MARK'
         ELSE CASE
          WHEN LOWER(price_type_code_ownership) = LOWER('C')
          THEN CASE
           WHEN LOWER(price_signal) = LOWER('01')
           THEN 'LAST CHANCE'
           WHEN LOWER(banner_code) = LOWER('N') AND LOWER(price_signal) = LOWER('97')
           THEN 'RACKING'
           WHEN clearance_markdown_version > 1
           THEN 'REMARK'
           WHEN clearance_markdown_version = 1
           THEN 'FIRST MARK'
           ELSE 'UNKNOWN'
           END
          WHEN LOWER(price_type_code_ownership) = LOWER('R')
          THEN 'REG PRICE'
          ELSE 'UNKNOWN'
          END
         END
        WHEN LOWER(price_type_code_ownership_future) = LOWER('R')
        THEN 'REG PRICE'
        ELSE 'UNKNOWN'
        END) = LOWER('REMARK')
     THEN '3. NORD '
     WHEN LOWER(CASE
        WHEN LOWER(price_type_code_ownership_future) = LOWER('C')
        THEN CASE
         WHEN LOWER(price_signal_future) = LOWER('01')
         THEN 'LAST CHANCE'
         WHEN LOWER(banner_code) = LOWER('N') AND LOWER(price_signal_future) = LOWER('97')
         THEN 'RACKING'
         WHEN LOWER(future_markdown_ind) = LOWER('Y') AND clearance_markdown_version > 0
         THEN 'REMARK'
         WHEN LOWER(future_markdown_ind) = LOWER('Y') AND clearance_markdown_version = 0
         THEN 'FIRST MARK'
         ELSE CASE
          WHEN LOWER(price_type_code_ownership) = LOWER('C')
          THEN CASE
           WHEN LOWER(price_signal) = LOWER('01')
           THEN 'LAST CHANCE'
           WHEN LOWER(banner_code) = LOWER('N') AND LOWER(price_signal) = LOWER('97')
           THEN 'RACKING'
           WHEN clearance_markdown_version > 1
           THEN 'REMARK'
           WHEN clearance_markdown_version = 1
           THEN 'FIRST MARK'
           ELSE 'UNKNOWN'
           END
          WHEN LOWER(price_type_code_ownership) = LOWER('R')
          THEN 'REG PRICE'
          ELSE 'UNKNOWN'
          END
         END
        WHEN LOWER(price_type_code_ownership_future) = LOWER('R')
        THEN 'REG PRICE'
        ELSE 'UNKNOWN'
        END) = LOWER('RACKING')
     THEN '4. NORD '
     WHEN LOWER(CASE
        WHEN LOWER(price_type_code_ownership_future) = LOWER('C')
        THEN CASE
         WHEN LOWER(price_signal_future) = LOWER('01')
         THEN 'LAST CHANCE'
         WHEN LOWER(banner_code) = LOWER('N') AND LOWER(price_signal_future) = LOWER('97')
         THEN 'RACKING'
         WHEN LOWER(future_markdown_ind) = LOWER('Y') AND clearance_markdown_version > 0
         THEN 'REMARK'
         WHEN LOWER(future_markdown_ind) = LOWER('Y') AND clearance_markdown_version = 0
         THEN 'FIRST MARK'
         ELSE CASE
          WHEN LOWER(price_type_code_ownership) = LOWER('C')
          THEN CASE
           WHEN LOWER(price_signal) = LOWER('01')
           THEN 'LAST CHANCE'
           WHEN LOWER(banner_code) = LOWER('N') AND LOWER(price_signal) = LOWER('97')
           THEN 'RACKING'
           WHEN clearance_markdown_version > 1
           THEN 'REMARK'
           WHEN clearance_markdown_version = 1
           THEN 'FIRST MARK'
           ELSE 'UNKNOWN'
           END
          WHEN LOWER(price_type_code_ownership) = LOWER('R')
          THEN 'REG PRICE'
          ELSE 'UNKNOWN'
          END
         END
        WHEN LOWER(price_type_code_ownership_future) = LOWER('R')
        THEN 'REG PRICE'
        ELSE 'UNKNOWN'
        END) = LOWER('LAST CHANCE')
     THEN '8. '
     ELSE NULL
     END
    WHEN LOWER(banner_code) = LOWER('R')
    THEN CASE
     WHEN LOWER(CASE
        WHEN LOWER(price_type_code_ownership_future) = LOWER('C')
        THEN CASE
         WHEN LOWER(price_signal_future) = LOWER('01')
         THEN 'LAST CHANCE'
         WHEN LOWER(banner_code) = LOWER('N') AND LOWER(price_signal_future) = LOWER('97')
         THEN 'RACKING'
         WHEN LOWER(future_markdown_ind) = LOWER('Y') AND clearance_markdown_version > 0
         THEN 'REMARK'
         WHEN LOWER(future_markdown_ind) = LOWER('Y') AND clearance_markdown_version = 0
         THEN 'FIRST MARK'
         ELSE CASE
          WHEN LOWER(price_type_code_ownership) = LOWER('C')
          THEN CASE
           WHEN LOWER(price_signal) = LOWER('01')
           THEN 'LAST CHANCE'
           WHEN LOWER(banner_code) = LOWER('N') AND LOWER(price_signal) = LOWER('97')
           THEN 'RACKING'
           WHEN clearance_markdown_version > 1
           THEN 'REMARK'
           WHEN clearance_markdown_version = 1
           THEN 'FIRST MARK'
           ELSE 'UNKNOWN'
           END
          WHEN LOWER(price_type_code_ownership) = LOWER('R')
          THEN 'REG PRICE'
          ELSE 'UNKNOWN'
          END
         END
        WHEN LOWER(price_type_code_ownership_future) = LOWER('R')
        THEN 'REG PRICE'
        ELSE 'UNKNOWN'
        END) = LOWER('REG PRICE')
     THEN '5. RACK '
     WHEN LOWER(CASE
        WHEN LOWER(price_type_code_ownership_future) = LOWER('C')
        THEN CASE
         WHEN LOWER(price_signal_future) = LOWER('01')
         THEN 'LAST CHANCE'
         WHEN LOWER(banner_code) = LOWER('N') AND LOWER(price_signal_future) = LOWER('97')
         THEN 'RACKING'
         WHEN LOWER(future_markdown_ind) = LOWER('Y') AND clearance_markdown_version > 0
         THEN 'REMARK'
         WHEN LOWER(future_markdown_ind) = LOWER('Y') AND clearance_markdown_version = 0
         THEN 'FIRST MARK'
         ELSE CASE
          WHEN LOWER(price_type_code_ownership) = LOWER('C')
          THEN CASE
           WHEN LOWER(price_signal) = LOWER('01')
           THEN 'LAST CHANCE'
           WHEN LOWER(banner_code) = LOWER('N') AND LOWER(price_signal) = LOWER('97')
           THEN 'RACKING'
           WHEN clearance_markdown_version > 1
           THEN 'REMARK'
           WHEN clearance_markdown_version = 1
           THEN 'FIRST MARK'
           ELSE 'UNKNOWN'
           END
          WHEN LOWER(price_type_code_ownership) = LOWER('R')
          THEN 'REG PRICE'
          ELSE 'UNKNOWN'
          END
         END
        WHEN LOWER(price_type_code_ownership_future) = LOWER('R')
        THEN 'REG PRICE'
        ELSE 'UNKNOWN'
        END) = LOWER('FIRST MARK')
     THEN '6. RACK '
     WHEN LOWER(CASE
        WHEN LOWER(price_type_code_ownership_future) = LOWER('C')
        THEN CASE
         WHEN LOWER(price_signal_future) = LOWER('01')
         THEN 'LAST CHANCE'
         WHEN LOWER(banner_code) = LOWER('N') AND LOWER(price_signal_future) = LOWER('97')
         THEN 'RACKING'
         WHEN LOWER(future_markdown_ind) = LOWER('Y') AND clearance_markdown_version > 0
         THEN 'REMARK'
         WHEN LOWER(future_markdown_ind) = LOWER('Y') AND clearance_markdown_version = 0
         THEN 'FIRST MARK'
         ELSE CASE
          WHEN LOWER(price_type_code_ownership) = LOWER('C')
          THEN CASE
           WHEN LOWER(price_signal) = LOWER('01')
           THEN 'LAST CHANCE'
           WHEN LOWER(banner_code) = LOWER('N') AND LOWER(price_signal) = LOWER('97')
           THEN 'RACKING'
           WHEN clearance_markdown_version > 1
           THEN 'REMARK'
           WHEN clearance_markdown_version = 1
           THEN 'FIRST MARK'
           ELSE 'UNKNOWN'
           END
          WHEN LOWER(price_type_code_ownership) = LOWER('R')
          THEN 'REG PRICE'
          ELSE 'UNKNOWN'
          END
         END
        WHEN LOWER(price_type_code_ownership_future) = LOWER('R')
        THEN 'REG PRICE'
        ELSE 'UNKNOWN'
        END) = LOWER('REMARK')
     THEN '7. RACK '
     WHEN LOWER(CASE
        WHEN LOWER(price_type_code_ownership_future) = LOWER('C')
        THEN CASE
         WHEN LOWER(price_signal_future) = LOWER('01')
         THEN 'LAST CHANCE'
         WHEN LOWER(banner_code) = LOWER('N') AND LOWER(price_signal_future) = LOWER('97')
         THEN 'RACKING'
         WHEN LOWER(future_markdown_ind) = LOWER('Y') AND clearance_markdown_version > 0
         THEN 'REMARK'
         WHEN LOWER(future_markdown_ind) = LOWER('Y') AND clearance_markdown_version = 0
         THEN 'FIRST MARK'
         ELSE CASE
          WHEN LOWER(price_type_code_ownership) = LOWER('C')
          THEN CASE
           WHEN LOWER(price_signal) = LOWER('01')
           THEN 'LAST CHANCE'
           WHEN LOWER(banner_code) = LOWER('N') AND LOWER(price_signal) = LOWER('97')
           THEN 'RACKING'
           WHEN clearance_markdown_version > 1
           THEN 'REMARK'
           WHEN clearance_markdown_version = 1
           THEN 'FIRST MARK'
           ELSE 'UNKNOWN'
           END
          WHEN LOWER(price_type_code_ownership) = LOWER('R')
          THEN 'REG PRICE'
          ELSE 'UNKNOWN'
          END
         END
        WHEN LOWER(price_type_code_ownership_future) = LOWER('R')
        THEN 'REG PRICE'
        ELSE 'UNKNOWN'
        END) = LOWER('LAST CHANCE')
     THEN '8. '
     ELSE NULL
     END
    ELSE NULL
    END || CASE
    WHEN LOWER(price_type_code_ownership_future) = LOWER('C')
    THEN CASE
     WHEN LOWER(price_signal_future) = LOWER('01')
     THEN 'LAST CHANCE'
     WHEN LOWER(banner_code) = LOWER('N') AND LOWER(price_signal_future) = LOWER('97')
     THEN 'RACKING'
     WHEN LOWER(future_markdown_ind) = LOWER('Y') AND clearance_markdown_version > 0
     THEN 'REMARK'
     WHEN LOWER(future_markdown_ind) = LOWER('Y') AND clearance_markdown_version = 0
     THEN 'FIRST MARK'
     ELSE CASE
      WHEN LOWER(price_type_code_ownership) = LOWER('C')
      THEN CASE
       WHEN LOWER(price_signal) = LOWER('01')
       THEN 'LAST CHANCE'
       WHEN LOWER(banner_code) = LOWER('N') AND LOWER(price_signal) = LOWER('97')
       THEN 'RACKING'
       WHEN clearance_markdown_version > 1
       THEN 'REMARK'
       WHEN clearance_markdown_version = 1
       THEN 'FIRST MARK'
       ELSE 'UNKNOWN'
       END
      WHEN LOWER(price_type_code_ownership) = LOWER('R')
      THEN 'REG PRICE'
      ELSE 'UNKNOWN'
      END
     END
    WHEN LOWER(price_type_code_ownership_future) = LOWER('R')
    THEN 'REG PRICE'
    ELSE 'UNKNOWN'
    END, 'UNKNOWN') AS lifecycle_phase_name_ownership_future,
 first_receipt_date,
 last_receipt_date,
 available_to_sell,
 first_sales_date,
 weeks_sales,
 total_sales_amt_1wk,
 total_sales_units_1wk,
 sell_thru_1wk,
 total_sales_amt_2wks,
 total_sales_units_2wks,
 sell_thru_2wks,
 total_sales_amt_4wks,
 total_sales_units_4wks,
 sell_thru_4wks,
 total_sales_amt_6mons,
 total_sales_units_6mons,
 sell_thru_6mons,
 sell_thru_since_last_markdown,
 total_inv_qty,
 total_inv_dollars,
 reserve_stock_inventory,
 reserve_stock_inventory_dollars,
 pack_and_hold_inventory,
 pack_and_hold_inventory_dollars,
 on_order_units_qty,
 reserve_stock_on_order_units_qty,
 pack_and_hold_on_order_units_qty
FROM add_price_type;
-- no stats recommended for collection
/*
* ADD FIELDS RELATED TO SALES, SELL-THRU AND TIME IN ASSORTMENT
*/
-- fields from prior query
/*
       * Sales
       */
/*
       * Sell-Thru
       * 
       * Not carrying forward 1wk, 2wks, 6mos because they are not use in reporting
       */
/*
       * Time in Assortment
       */
/*
       * Remaining CDS fields to be handled in later queries
       */
CREATE TEMPORARY TABLE IF NOT EXISTS add_selling_info AS
SELECT snapshot_date,
 channel_country,
 channel_brand,
 banner_code,
 selling_channel,
 cc,
 banner_channel_cc,
 country_banner_channel_cc,
 cc_dsci,
 div_num,
 div_label,
 subdiv_num,
 subdiv_label,
 dept_num,
 dept_label,
 class_num,
 class_label,
 subclass_num,
 subclass_label,
 style_group_num,
 rms_style_num,
 color_num,
 vpn,
 supp_color,
 style_desc,
 prmy_supp_num,
 vendor_name,
 vendor_label_name,
 npg_ind,
 promotion_ind,
 anniversary_item_ind,
 replenishment_eligible_ind,
 drop_ship_eligible_ind,
 return_disposition_code,
 selling_status_code_legacy,
 first_rack_date,
 rack_ind,
 online_purchasable_ind,
 online_purchasable_eff_begin_date,
 online_purchasable_eff_end_date,
 average_unit_cost,
 price_base,
 price_ownership,
 price_ownership_future,
 price_selling,
 price_base_drop_ship,
 price_compare_at,
 price_current_rack,
 price_variance_within_cc_ind,
 price_base_next_highest_dollar,
 price_ownership_next_highest_dollar,
 price_selling_next_highest_dollar,
 price_ownership_future_next_highest_dollar,
 first_clearance_markdown_date,
 last_clearance_markdown_date,
 future_clearance_markdown_date,
 clearance_markdown_version,
 clearance_markdown_state,
 future_markdown_ind,
 weeks_since_last_markdown,
 pct_off_ownership_vs_base,
 pct_off_selling_vs_base,
 pct_off_ownership_future_vs_base,
 pct_off_ownership_vs_compare_at,
 pct_off_ownership_future_vs_compare_at,
 pct_off_band_ownership_vs_base,
 pct_off_band_selling_vs_base,
 pct_off_band_ownership_future_vs_base,
 pct_off_band_ownership_vs_compare_at,
 pct_off_band_ownership_future_vs_compare_at,
 price_type_code_ownership,
 price_type_code_ownership_future,
 price_type_code_selling,
 price_type_ownership,
 price_type_ownership_future,
 price_type_selling,
 price_signal,
 price_signal_future,
 markdown_type_ownership,
 markdown_type_ownership_future,
 lifecycle_phase_name_ownership,
 lifecycle_phase_name_ownership_future,
 total_sales_amt_1wk AS sales_retail_1wk,
 total_sales_units_1wk AS sales_units_1wk,
 total_sales_amt_2wks AS sales_retail_2wk,
 total_sales_units_2wks AS sales_units_2wk,
 total_sales_amt_4wks AS sales_retail_4wk,
 total_sales_units_4wks AS sales_units_4wk,
 total_sales_amt_6mons AS sales_retail_6mo,
 total_sales_units_6mons AS sales_units_6mo,
 sell_thru_4wks AS sell_thru_4wk,
 sell_thru_since_last_markdown,
  CAST(sell_thru_4wks AS NUMERIC) / 4 AS sell_thru_4wk_avg_wkly,
  CASE
  WHEN CAST(sell_thru_4wks AS NUMERIC) IS NULL
  THEN NULL
  WHEN CAST(sell_thru_4wks AS NUMERIC) / 4 < 0
  THEN '< 0%'
  WHEN CAST(sell_thru_4wks AS NUMERIC) / 4 <= 0.05
  THEN '00-05%'
  WHEN CAST(sell_thru_4wks AS NUMERIC) / 4 <= 0.10
  THEN '06-10%'
  WHEN CAST(sell_thru_4wks AS NUMERIC) / 4 <= 0.15
  THEN '11-15%'
  WHEN CAST(sell_thru_4wks AS NUMERIC) / 4 <= 0.20
  THEN '16-20%'
  WHEN CAST(sell_thru_4wks AS NUMERIC) / 4 <= 0.25
  THEN '21-25%'
  WHEN CAST(sell_thru_4wks AS NUMERIC) / 4 <= 0.30
  THEN '26-30%'
  WHEN CAST(sell_thru_4wks AS NUMERIC) / 4 <= 0.35
  THEN '31-35%'
  WHEN CAST(sell_thru_4wks AS NUMERIC) / 4 <= 0.40
  THEN '36-40%'
  ELSE '41%+'
  END AS sell_thru_band_4wk_avg_wkly,
  CASE
  WHEN weeks_since_last_markdown = 0
  THEN 0
  ELSE CAST(sell_thru_since_last_markdown AS NUMERIC) / weeks_since_last_markdown
  END AS sell_thru_since_last_markdown_avg_wkly,
  CASE
  WHEN CASE
   WHEN weeks_since_last_markdown = 0
   THEN 0
   ELSE CAST(sell_thru_since_last_markdown AS NUMERIC) / weeks_since_last_markdown
   END IS NULL
  THEN NULL
  WHEN CASE
    WHEN weeks_since_last_markdown = 0
    THEN 0
    ELSE CAST(sell_thru_since_last_markdown AS NUMERIC) / weeks_since_last_markdown
    END < 0
  THEN '< 0%'
  WHEN CASE
    WHEN weeks_since_last_markdown = 0
    THEN 0
    ELSE CAST(sell_thru_since_last_markdown AS NUMERIC) / weeks_since_last_markdown
    END <= 0.05
  THEN '00-05%'
  WHEN CASE
    WHEN weeks_since_last_markdown = 0
    THEN 0
    ELSE CAST(sell_thru_since_last_markdown AS NUMERIC) / weeks_since_last_markdown
    END <= 0.10
  THEN '06-10%'
  WHEN CASE
    WHEN weeks_since_last_markdown = 0
    THEN 0
    ELSE CAST(sell_thru_since_last_markdown AS NUMERIC) / weeks_since_last_markdown
    END <= 0.15
  THEN '11-15%'
  WHEN CASE
    WHEN weeks_since_last_markdown = 0
    THEN 0
    ELSE CAST(sell_thru_since_last_markdown AS NUMERIC) / weeks_since_last_markdown
    END <= 0.20
  THEN '16-20%'
  WHEN CASE
    WHEN weeks_since_last_markdown = 0
    THEN 0
    ELSE CAST(sell_thru_since_last_markdown AS NUMERIC) / weeks_since_last_markdown
    END <= 0.25
  THEN '21-25%'
  WHEN CASE
    WHEN weeks_since_last_markdown = 0
    THEN 0
    ELSE CAST(sell_thru_since_last_markdown AS NUMERIC) / weeks_since_last_markdown
    END <= 0.30
  THEN '26-30%'
  WHEN CASE
    WHEN weeks_since_last_markdown = 0
    THEN 0
    ELSE CAST(sell_thru_since_last_markdown AS NUMERIC) / weeks_since_last_markdown
    END <= 0.35
  THEN '31-35%'
  WHEN CASE
    WHEN weeks_since_last_markdown = 0
    THEN 0
    ELSE CAST(sell_thru_since_last_markdown AS NUMERIC) / weeks_since_last_markdown
    END <= 0.40
  THEN '36-40%'
  ELSE '41%+'
  END AS sell_thru_band_since_last_markdown_avg_wkly,
 first_receipt_date,
 last_receipt_date,
 trunc(CAST(trunc(DATE_DIFF(snapshot_date, last_receipt_date, DAY) / 7) AS INT64)) AS weeks_since_last_receipt_date,
  CASE
  WHEN trunc(CAST(trunc(DATE_DIFF(snapshot_date, last_receipt_date, DAY) / 7) AS INT64)) < 5
  THEN '0 - 4'
  WHEN trunc(CAST(trunc(DATE_DIFF(snapshot_date, last_receipt_date, DAY) / 7) AS INT64)) < 9
  THEN '5 - 8'
  WHEN trunc(CAST(trunc(DATE_DIFF(snapshot_date, last_receipt_date, DAY) / 7) AS INT64)) < 13
  THEN '9 - 12'
  WHEN trunc(CAST(trunc(DATE_DIFF(snapshot_date, last_receipt_date, DAY) / 7) AS INT64)) < 17
  THEN '13 - 16'
  WHEN trunc(CAST(trunc(DATE_DIFF(snapshot_date, last_receipt_date, DAY) / 7) AS INT64)) < 27
  THEN '17 - 26'
  WHEN trunc(CAST(trunc(DATE_DIFF(snapshot_date, last_receipt_date, DAY) / 7) AS INT64)) < 52
  THEN '27 - 51'
  ELSE '52+'
  END AS weeks_since_last_receipt_date_band,
 available_to_sell AS weeks_available_to_sell,
  CASE
  WHEN available_to_sell IS NULL
  THEN 'UNKNOWN'
  WHEN available_to_sell < 5
  THEN '0 - 4'
  WHEN available_to_sell < 9
  THEN '5 - 8'
  WHEN available_to_sell < 13
  THEN '9 - 12'
  WHEN available_to_sell < 17
  THEN '13 - 16'
  WHEN available_to_sell < 27
  THEN '17 - 26'
  WHEN available_to_sell < 52
  THEN '27 - 51'
  ELSE '52+'
  END AS weeks_available_to_sell_band,
 first_sales_date,
 weeks_sales AS weeks_since_first_sale,
  CASE
  WHEN weeks_sales IS NULL
  THEN 'UNKNOWN'
  WHEN weeks_sales < 5
  THEN '0 - 4'
  WHEN weeks_sales < 9
  THEN '5 - 8'
  WHEN weeks_sales < 13
  THEN '9 - 12'
  WHEN weeks_sales < 17
  THEN '13 - 16'
  WHEN weeks_sales < 27
  THEN '17 - 26'
  WHEN weeks_sales < 52
  THEN '27 - 51'
  ELSE '52+'
  END AS weeks_since_first_sale_band,
 total_inv_qty,
 total_inv_dollars,
 reserve_stock_inventory,
 reserve_stock_inventory_dollars,
 pack_and_hold_inventory,
 pack_and_hold_inventory_dollars,
 on_order_units_qty,
 reserve_stock_on_order_units_qty,
 pack_and_hold_on_order_units_qty
FROM add_lifecycle_phase;
-- no stats recommended for collection
/*
* ADD FIELDS RELATED TO INVENTORY, ON ORDER, MARKDOWN DOLLARS
*/
-- fields from prior query
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
/*
       * On Order
       * Null handling: units come from CDS directly so NULL has same meaning as zero (no on order present)
       */
/*
       * Markdown Dollars
       * 
       * Null handling: same as Inventory section above
       * Casting rounds to nearest two decimals 
       */
CREATE TEMPORARY TABLE IF NOT EXISTS add_inventory AS
SELECT snapshot_date,
 channel_country,
 channel_brand,
 banner_code,
 selling_channel,
 cc,
 banner_channel_cc,
 country_banner_channel_cc,
 cc_dsci,
 div_num,
 div_label,
 subdiv_num,
 subdiv_label,
 dept_num,
 dept_label,
 class_num,
 class_label,
 subclass_num,
 subclass_label,
 style_group_num,
 rms_style_num,
 color_num,
 vpn,
 supp_color,
 style_desc,
 prmy_supp_num,
 vendor_name,
 vendor_label_name,
 npg_ind,
 promotion_ind,
 anniversary_item_ind,
 replenishment_eligible_ind,
 drop_ship_eligible_ind,
 return_disposition_code,
 selling_status_code_legacy,
 first_rack_date,
 rack_ind,
 online_purchasable_ind,
 online_purchasable_eff_begin_date,
 online_purchasable_eff_end_date,
 average_unit_cost,
 price_base,
 price_ownership,
 price_ownership_future,
 price_selling,
 price_base_drop_ship,
 price_compare_at,
 price_current_rack,
 price_variance_within_cc_ind,
 price_base_next_highest_dollar,
 price_ownership_next_highest_dollar,
 price_selling_next_highest_dollar,
 price_ownership_future_next_highest_dollar,
 first_clearance_markdown_date,
 last_clearance_markdown_date,
 future_clearance_markdown_date,
 clearance_markdown_version,
 clearance_markdown_state,
 future_markdown_ind,
 weeks_since_last_markdown,
 pct_off_ownership_vs_base,
 pct_off_selling_vs_base,
 pct_off_ownership_future_vs_base,
 pct_off_ownership_vs_compare_at,
 pct_off_ownership_future_vs_compare_at,
 pct_off_band_ownership_vs_base,
 pct_off_band_selling_vs_base,
 pct_off_band_ownership_future_vs_base,
 pct_off_band_ownership_vs_compare_at,
 pct_off_band_ownership_future_vs_compare_at,
 price_type_code_ownership,
 price_type_code_ownership_future,
 price_type_code_selling,
 price_type_ownership,
 price_type_ownership_future,
 price_type_selling,
 price_signal,
 price_signal_future,
 markdown_type_ownership,
 markdown_type_ownership_future,
 lifecycle_phase_name_ownership,
 lifecycle_phase_name_ownership_future,
 sales_retail_1wk,
 sales_units_1wk,
 sales_retail_2wk,
 sales_units_2wk,
 sales_retail_4wk,
 sales_units_4wk,
 sales_retail_6mo,
 sales_units_6mo,
 sell_thru_4wk,
 sell_thru_since_last_markdown,
 sell_thru_4wk_avg_wkly,
 sell_thru_band_4wk_avg_wkly,
 sell_thru_since_last_markdown_avg_wkly,
 sell_thru_band_since_last_markdown_avg_wkly,
 first_receipt_date,
 last_receipt_date,
 weeks_since_last_receipt_date,
 weeks_since_last_receipt_date_band,
 weeks_available_to_sell,
 weeks_available_to_sell_band,
 first_sales_date,
 weeks_since_first_sale,
 weeks_since_first_sale_band,
 COALESCE(total_inv_qty, 0) AS eop_units_selling_locations,
 COALESCE(total_inv_dollars, 0) AS eop_retail_selling_locations,
 ROUND(CAST(COALESCE(total_inv_qty, 0) * average_unit_cost AS NUMERIC), 2) AS eop_cost_selling_locations,
 CAST(COALESCE(total_inv_qty, 0) * price_ownership_future AS NUMERIC) AS eop_retail_future_selling_locations,
 COALESCE(reserve_stock_inventory, 0) AS eop_units_reserve_stock,
 COALESCE(reserve_stock_inventory_dollars, 0) AS eop_retail_reserve_stock,
 ROUND(CAST(COALESCE(reserve_stock_inventory, 0) * average_unit_cost AS NUMERIC), 2) AS eop_cost_reserve_stock,
 CAST(COALESCE(reserve_stock_inventory, 0) * price_ownership_future AS NUMERIC) AS eop_retail_future_reserve_stock,
 COALESCE(pack_and_hold_inventory, 0) AS eop_units_pack_and_hold,
 COALESCE(pack_and_hold_inventory_dollars, 0) AS eop_retail_pack_and_hold,
 ROUND(CAST(COALESCE(pack_and_hold_inventory, 0) * average_unit_cost AS NUMERIC), 2) AS eop_cost_pack_and_hold,
 CAST(COALESCE(pack_and_hold_inventory, 0) * price_ownership_future AS NUMERIC) AS eop_retail_future_pack_and_hold,
   COALESCE(total_inv_qty, 0) + COALESCE(reserve_stock_inventory, 0) + COALESCE(pack_and_hold_inventory, 0) AS
 eop_units_all_locations,
   COALESCE(total_inv_dollars, 0) + COALESCE(reserve_stock_inventory_dollars, 0) + COALESCE(pack_and_hold_inventory_dollars
   , 0) AS eop_retail_all_locations,
   ROUND(CAST(COALESCE(total_inv_qty, 0) * average_unit_cost AS NUMERIC), 2) + ROUND(CAST(COALESCE(reserve_stock_inventory, 0) * average_unit_cost AS NUMERIC)
    , 2) + ROUND(CAST(COALESCE(pack_and_hold_inventory, 0) * average_unit_cost AS NUMERIC), 2) AS eop_cost_all_locations
 ,
   CAST(COALESCE(total_inv_qty, 0) * price_ownership_future AS NUMERIC) + CAST(COALESCE(reserve_stock_inventory, 0) * price_ownership_future AS NUMERIC)
    + CAST(COALESCE(pack_and_hold_inventory, 0) * price_ownership_future AS NUMERIC) AS eop_retail_future_all_locations
 ,
 COALESCE(on_order_units_qty, 0) AS on_order_units_selling_locations,
 COALESCE(reserve_stock_on_order_units_qty, 0) AS on_order_units_reserve_stock,
 COALESCE(pack_and_hold_on_order_units_qty, 0) AS on_order_units_pack_and_hold,
   COALESCE(on_order_units_qty, 0) + COALESCE(reserve_stock_on_order_units_qty, 0) + COALESCE(pack_and_hold_on_order_units_qty
   , 0) AS on_order_units_all_locations,
 CAST((price_base - price_ownership) * COALESCE(total_inv_qty, 0) AS NUMERIC) AS markdown_dollars_selling_locations,
 CAST((price_base - price_ownership) * COALESCE(pack_and_hold_inventory, 0) AS NUMERIC) AS
 markdown_dollars_pack_and_hold,
 CAST((price_base - price_ownership) * COALESCE(reserve_stock_inventory, 0) AS NUMERIC) AS
 markdown_dollars_reserve_stock,
 CAST((price_base - price_ownership) * (COALESCE(total_inv_qty, 0) + COALESCE(reserve_stock_inventory, 0) + COALESCE(pack_and_hold_inventory, 0)) AS NUMERIC)
 AS markdown_dollars_all_locations,
 CAST((price_base - price_ownership_future) * COALESCE(total_inv_qty, 0) AS NUMERIC) AS
 markdown_dollars_future_selling_locations,
 CAST((price_base - price_ownership_future) * COALESCE(pack_and_hold_inventory, 0) AS NUMERIC) AS
 markdown_dollars_future_pack_and_hold,
 CAST((price_base - price_ownership_future) * COALESCE(reserve_stock_inventory, 0) AS NUMERIC) AS
 markdown_dollars_future_reserve_stock,
 CAST((price_base - price_ownership_future) * (COALESCE(total_inv_qty, 0) + COALESCE(reserve_stock_inventory, 0) + COALESCE(pack_and_hold_inventory, 0)) AS NUMERIC)
 AS markdown_dollars_future_all_locations
FROM add_selling_info;
--collect statistics    column( snapshot_date, banner_channel_cc ) on add_inventory 
/*
 * ADD FIELDS FROM OPPOSITE BANNER
 * 
 * First select specific fields from current data
 * Then join them back to create new columns that provide opposite-banner info on same row
 */
-- opposite-banner code will enable join back to main data set
CREATE TEMPORARY TABLE IF NOT EXISTS other_banner AS
SELECT snapshot_date,
      CASE
      WHEN LOWER(banner_code) = LOWER('N')
      THEN 'R'
      WHEN LOWER(banner_code) = LOWER('R')
      THEN 'N'
      ELSE NULL
      END || '-' || selling_channel || '-' || cc AS other_banner_channel_cc,
 drop_ship_eligible_ind AS drop_ship_eligible_ind_other_banner,
 replenishment_eligible_ind AS replenishment_eligible_ind_other_banner,
 price_type_ownership AS price_type_ownership_other_banner,
 price_type_ownership_future AS price_type_ownership_future_other_banner,
 price_type_selling AS price_type_selling_other_banner,
 price_base AS price_base_other_banner,
 price_ownership AS price_ownership_other_banner,
 price_ownership_future AS price_ownership_future_other_banner,
 price_selling AS price_selling_other_banner,
 future_clearance_markdown_date AS future_clearance_markdown_date_other_banner,
 future_markdown_ind AS future_markdown_ind_other_banner,
 sell_thru_4wk AS sell_thru_4wk_other_banner,
 sell_thru_since_last_markdown AS sell_thru_since_last_markdown_other_banner,
 sell_thru_4wk_avg_wkly AS sell_thru_4wk_avg_wkly_other_banner,
 sell_thru_band_4wk_avg_wkly AS sell_thru_band_4wk_avg_wkly_other_banner,
 sell_thru_since_last_markdown_avg_wkly AS sell_thru_since_last_markdown_avg_wkly_other_banner,
 sell_thru_band_since_last_markdown_avg_wkly AS sell_thru_band_since_last_markdown_avg_wkly_other_banner,
 eop_units_selling_locations AS eop_units_selling_locations_other_banner,
 eop_retail_selling_locations AS eop_retail_selling_locations_other_banner,
 eop_cost_selling_locations AS eop_cost_selling_locations_other_banner,
 eop_units_all_locations AS eop_units_all_locations_other_banner,
 eop_retail_all_locations AS eop_retail_all_locations_other_banner,
 eop_cost_all_locations AS eop_cost_all_locations_other_banner
FROM add_inventory;
--collect statistics    column( snapshot_date, other_banner_channel_cc ) on other_banner 
CREATE TEMPORARY TABLE IF NOT EXISTS add_other_banner AS
SELECT a.snapshot_date,
 a.channel_country,
 a.channel_brand,
 a.banner_code,
 a.selling_channel,
 a.cc,
 a.banner_channel_cc,
 a.country_banner_channel_cc,
 a.cc_dsci,
 a.div_num,
 a.div_label,
 a.subdiv_num,
 a.subdiv_label,
 a.dept_num,
 a.dept_label,
 a.class_num,
 a.class_label,
 a.subclass_num,
 a.subclass_label,
 a.style_group_num,
 a.rms_style_num,
 a.color_num,
 a.vpn,
 a.supp_color,
 a.style_desc,
 a.prmy_supp_num,
 a.vendor_name,
 a.vendor_label_name,
 a.npg_ind,
 a.promotion_ind,
 a.anniversary_item_ind,
 a.replenishment_eligible_ind,
 a.drop_ship_eligible_ind,
 a.return_disposition_code,
 a.selling_status_code_legacy,
 a.first_rack_date,
 a.rack_ind,
 a.online_purchasable_ind,
 a.online_purchasable_eff_begin_date,
 a.online_purchasable_eff_end_date,
 a.average_unit_cost,
 a.price_base,
 a.price_ownership,
 a.price_ownership_future,
 a.price_selling,
 a.price_base_drop_ship,
 a.price_compare_at,
 a.price_current_rack,
 a.price_variance_within_cc_ind,
 a.price_base_next_highest_dollar,
 a.price_ownership_next_highest_dollar,
 a.price_selling_next_highest_dollar,
 a.price_ownership_future_next_highest_dollar,
 a.first_clearance_markdown_date,
 a.last_clearance_markdown_date,
 a.future_clearance_markdown_date,
 a.clearance_markdown_version,
 a.clearance_markdown_state,
 a.future_markdown_ind,
 a.weeks_since_last_markdown,
 a.pct_off_ownership_vs_base,
 a.pct_off_selling_vs_base,
 a.pct_off_ownership_future_vs_base,
 a.pct_off_ownership_vs_compare_at,
 a.pct_off_ownership_future_vs_compare_at,
 a.pct_off_band_ownership_vs_base,
 a.pct_off_band_selling_vs_base,
 a.pct_off_band_ownership_future_vs_base,
 a.pct_off_band_ownership_vs_compare_at,
 a.pct_off_band_ownership_future_vs_compare_at,
 a.price_type_code_ownership,
 a.price_type_code_ownership_future,
 a.price_type_code_selling,
 a.price_type_ownership,
 a.price_type_ownership_future,
 a.price_type_selling,
 a.price_signal,
 a.price_signal_future,
 a.markdown_type_ownership,
 a.markdown_type_ownership_future,
 a.lifecycle_phase_name_ownership,
 a.lifecycle_phase_name_ownership_future,
 a.sales_retail_1wk,
 a.sales_units_1wk,
 a.sales_retail_2wk,
 a.sales_units_2wk,
 a.sales_retail_4wk,
 a.sales_units_4wk,
 a.sales_retail_6mo,
 a.sales_units_6mo,
 a.sell_thru_4wk,
 a.sell_thru_since_last_markdown,
 a.sell_thru_4wk_avg_wkly,
 a.sell_thru_band_4wk_avg_wkly,
 a.sell_thru_since_last_markdown_avg_wkly,
 a.sell_thru_band_since_last_markdown_avg_wkly,
 a.first_receipt_date,
 a.last_receipt_date,
 a.weeks_since_last_receipt_date,
 a.weeks_since_last_receipt_date_band,
 a.weeks_available_to_sell,
 a.weeks_available_to_sell_band,
 a.first_sales_date,
 a.weeks_since_first_sale,
 a.weeks_since_first_sale_band,
 a.eop_units_selling_locations,
 a.eop_retail_selling_locations,
 a.eop_cost_selling_locations,
 a.eop_retail_future_selling_locations,
 a.eop_units_reserve_stock,
 a.eop_retail_reserve_stock,
 a.eop_cost_reserve_stock,
 a.eop_retail_future_reserve_stock,
 a.eop_units_pack_and_hold,
 a.eop_retail_pack_and_hold,
 a.eop_cost_pack_and_hold,
 a.eop_retail_future_pack_and_hold,
 a.eop_units_all_locations,
 a.eop_retail_all_locations,
 a.eop_cost_all_locations,
 a.eop_retail_future_all_locations,
 a.on_order_units_selling_locations,
 a.on_order_units_reserve_stock,
 a.on_order_units_pack_and_hold,
 a.on_order_units_all_locations,
 a.markdown_dollars_selling_locations,
 a.markdown_dollars_pack_and_hold,
 a.markdown_dollars_reserve_stock,
 a.markdown_dollars_all_locations,
 a.markdown_dollars_future_selling_locations,
 a.markdown_dollars_future_pack_and_hold,
 a.markdown_dollars_future_reserve_stock,
 a.markdown_dollars_future_all_locations,
 ob.drop_ship_eligible_ind_other_banner,
 ob.replenishment_eligible_ind_other_banner,
 ob.price_type_ownership_other_banner,
 ob.price_type_ownership_future_other_banner,
 ob.price_type_selling_other_banner,
 ob.price_base_other_banner,
 ob.price_ownership_other_banner,
 ob.price_ownership_future_other_banner,
 ob.price_selling_other_banner,
 ob.future_clearance_markdown_date_other_banner,
 ob.future_markdown_ind_other_banner,
 ob.sell_thru_4wk_other_banner,
 ob.sell_thru_since_last_markdown_other_banner,
 ob.sell_thru_4wk_avg_wkly_other_banner,
 ob.sell_thru_band_4wk_avg_wkly_other_banner,
 ob.sell_thru_since_last_markdown_avg_wkly_other_banner,
 ob.sell_thru_band_since_last_markdown_avg_wkly_other_banner,
 ob.eop_units_selling_locations_other_banner,
 ob.eop_retail_selling_locations_other_banner,
 ob.eop_cost_selling_locations_other_banner,
 ob.eop_units_all_locations_other_banner,
 ob.eop_retail_all_locations_other_banner,
 ob.eop_cost_all_locations_other_banner
FROM add_inventory AS a
 LEFT JOIN other_banner AS ob ON a.snapshot_date = ob.snapshot_date AND LOWER(ob.other_banner_channel_cc) = LOWER(a.banner_channel_cc
    );
--collect statistics    column (dept_num ,class_num ,subclass_num)    ,column (banner_channel_cc)    ,column (snapshot_date ,banner_channel_cc)    ,column (banner_code ,dept_num)    ,column (prmy_supp_num)    ,column (banner_code ,dept_num, prmy_supp_num) on add_other_banner 
/*
 * CATEGORY AND SEASON
 */
-- Table contains records at Dept + Class + Subclass and Dept + Class
-- If category has been assigned at Dept + Class then the table will have subclass = -1 
-- We use the Dept + Class + Subclass record if it exists, and if it does not then we use the Dept + Class record
CREATE TEMPORARY TABLE IF NOT EXISTS category_lkup AS
SELECT DISTINCT h.dept_num,
 h.class_num,
 h.sbclass_num,
 COALESCE(cat1.category, cat2.category, 'UNKNOWN') AS quantrix_category,
 COALESCE(cat1.category_group, cat2.category_group, 'UNKNOWN') AS quantrix_category_group,
 COALESCE(cat1.seasonal_designation, cat2.seasonal_designation, 'NONE') AS quantrix_season
FROM (SELECT DISTINCT dept_num,
   class_num,
   sbclass_num
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw
  WHERE LOWER(channel_country) = LOWER('US')) AS h
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.catg_subclass_map_dim AS cat1 ON h.dept_num = cat1.dept_num AND h.class_num = CAST(cat1.class_num AS FLOAT64)
     AND h.sbclass_num = CAST(cat1.sbclass_num AS FLOAT64)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.catg_subclass_map_dim AS cat2 ON h.dept_num = cat2.dept_num AND h.class_num = CAST(cat2.class_num AS FLOAT64)
     AND CAST(cat2.sbclass_num AS FLOAT64) = - 1;
--collect statistics   column (dept_num, class_num, sbclass_num)  on category_lkup 
/*
 * ANCHOR BRANDS
 */
CREATE TEMPORARY TABLE IF NOT EXISTS anchor_brands_lkup AS
SELECT DISTINCT CASE
  WHEN LOWER(banner) = LOWER('NORDSTROM')
  THEN 'N'
  WHEN LOWER(banner) = LOWER('NORDSTROM_RACK')
  THEN 'R'
  ELSE NULL
  END AS banner_code,
 dept_num,
 supplier_idnt,
 anchor_brand_name
FROM `{{params.gcp_project_id}}`.t2dl_das_in_season_management_reporting.anchor_brands;
--collect statistics   column (banner_code, dept_num, supplier_idnt)   ,column (supplier_idnt) on anchor_brands_lkup 
/*
 * SELLING RIGHTS
 * 
 * First query: fetch data from NAP table for STORE and ONLINE channels
 * Second query: up-level STORE and ONLINE channels to same grain as CDS
 * Third query: determine datat for OMNI channel
 * Fourth query: put all channels in one table
 */
CREATE TEMPORARY TABLE IF NOT EXISTS selling_rights_lkup AS
SELECT CASE
        WHEN LOWER(sell.channel_brand) = LOWER('NORDSTROM')
        THEN 'N'
        WHEN LOWER(sell.channel_brand) = LOWER('NORDSTROM_RACK')
        THEN 'R'
        ELSE NULL
        END || '-' || sell.selling_channel || '-' || sku.rms_style_num || '-' || sku.color_num AS banner_channel_cc,
 sku.rms_sku_num,
 sku.rms_style_num,
 sku.color_num,
  CASE
  WHEN LOWER(sell.channel_brand) = LOWER('NORDSTROM')
  THEN 'N'
  WHEN LOWER(sell.channel_brand) = LOWER('NORDSTROM_RACK')
  THEN 'R'
  ELSE NULL
  END AS banner_code,
 sell.selling_channel,
 sell.is_sellable_ind,
 sell.selling_status_code,
 sell.eff_begin_tmstp,
 sell.eff_end_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_item_selling_rights_dim AS sell
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS sku ON LOWER(sku.rms_sku_num) = LOWER(sell.rms_sku_num) AND LOWER(sku
    .channel_country) = LOWER(sell.channel_country)
WHERE LOWER(sell.channel_country) = LOWER('US');
--collect statistics    column( rms_sku_num, banner_channel_cc, eff_begin_tmstp ) on selling_rights_lkup 
-- max of (Y,N) will be Y if any SKU in the CC has selling rights
-- max of (UNBLOCKED,BLOCKED) will be UNBLOCKED if any SKU in the CC is unblocked
CREATE TEMPORARY TABLE IF NOT EXISTS selling_rights_stg_part1 AS 
WITH cds_dims AS 
(SELECT snapshot_date,
   banner_channel_cc,
   banner_code,
   selling_channel,
   cc
  FROM add_other_banner
  WHERE LOWER(selling_channel) <> LOWER('OMNI')) 
  (SELECT cds.snapshot_date,
   cds.banner_channel_cc,
   cds.banner_code,
   cds.selling_channel,
   cds.cc,
   MAX(s.is_sellable_ind) AS selling_rights_ind,
   MAX(s.selling_status_code) AS selling_status_code
  FROM cds_dims AS cds
   LEFT JOIN selling_rights_lkup AS s ON LOWER(s.banner_channel_cc) = LOWER(cds.banner_channel_cc) AND cds.snapshot_date
     BETWEEN CAST(s.eff_begin_tmstp AS DATE) AND (CAST(s.eff_end_tmstp AS DATE))
  GROUP BY cds.snapshot_date,
   cds.banner_channel_cc,
   cds.banner_code,
   cds.selling_channel,
   cds.cc);
-- max of (Y,N) will be Y if any channel has selling rights
-- max of (UNBLOCKED,BLOCKED) will be UNBLOCKED if any channel is unblocked
CREATE TEMPORARY TABLE IF NOT EXISTS selling_rights_stg_part2 AS
SELECT snapshot_date,
     banner_code || '-' || 'OMNI' || '-' || cc AS banner_channel_cc,
 banner_code,
 'OMNI' AS selling_channel,
 cc,
 MAX(selling_rights_ind) AS selling_rights_ind,
 MAX(selling_status_code) AS selling_status_code
FROM selling_rights_stg_part1 AS sr1
GROUP BY snapshot_date,
 banner_channel_cc,
 banner_code,
 selling_channel,
 cc;
CREATE TEMPORARY TABLE IF NOT EXISTS selling_rights_stg
AS
SELECT *
FROM selling_rights_stg_part1
UNION ALL
SELECT *
FROM selling_rights_stg_part2;
--collect statistics    column( snapshot_date, banner_channel_cc ) on selling_rights_stg 
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
-- month_idnt formatted in standard way: 202406 for July FY24
-- week_idnt formatted in standard way: 202406 for 6th week of FY24
CREATE TEMPORARY TABLE IF NOT EXISTS item_intent_lkup AS
SELECT CASE
        WHEN LOWER(channel_brand) = LOWER('NORDSTROM')
        THEN 'N'
        WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
        THEN 'R'
        ELSE NULL
        END || '-' || 'OMNI' || '-' || rms_style_num || '-' || sku_nrf_color_num AS banner_channel_cc,
  CASE
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM')
  THEN 'N'
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
  THEN 'R'
  ELSE NULL
  END AS banner_code,
 channel_brand,
 'OMNI' AS selling_channel,
 rms_style_num,
 sku_nrf_color_num AS color_num,
 intended_season AS intent_season,
 intended_lifecycle_type AS intent_lifecycle_type,
 scaled_event AS intent_scaled_event,
 holiday_or_celebration AS intent_holiday_or_celebration,
 CAST(CASE
   WHEN SUBSTR(intended_exit_month_year, 4, 4) || SUBSTR(intended_exit_month_year, 1, 2) = ''
   THEN '0'
   ELSE SUBSTR(intended_exit_month_year, 4, 4) || SUBSTR(intended_exit_month_year, 1, 2)
   END AS INTEGER) AS intent_exit_month_idnt,
 CAST(CASE
   WHEN CONCAT(TRIM(FORMAT('%6d', plan_year)), TRIM(CASE
       WHEN LENGTH(SUBSTR(plan_week, STRPOS(LOWER(plan_week), LOWER('_')) + 1)) = 1
       THEN FORMAT('%4d', 0) || SUBSTR(plan_week, (STRPOS(LOWER(plan_week), LOWER('_')) + 1))
       ELSE SUBSTR(plan_week, STRPOS(LOWER(plan_week), LOWER('_')) + 1)
       END)) = ''
   THEN '0'
   ELSE CONCAT(TRIM(FORMAT('%6d', plan_year)), TRIM(CASE
      WHEN LENGTH(SUBSTR(plan_week, STRPOS(LOWER(plan_week), LOWER('_')) + 1)) = 1
      THEN FORMAT('%4d', 0) || SUBSTR(plan_week, (STRPOS(LOWER(plan_week), LOWER('_')) + 1))
      ELSE SUBSTR(plan_week, STRPOS(LOWER(plan_week), LOWER('_')) + 1)
      END))
   END AS INTEGER) AS intent_plan_week_idnt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.item_intent_plan_fact_enhanced_smart_markdown_vw
WHERE LOWER(channel_country) = LOWER('US');
--collect statistics    column( banner_channel_cc ) on item_intent_lkup 
/*
 * ADD CATEGORY, ANCHOR BRAND, SELLING RIGHTS, ITEM INTENT
 */
CREATE TEMPORARY TABLE IF NOT EXISTS add_nap_dims AS
SELECT b.snapshot_date,
 b.channel_country,
 b.channel_brand,
 b.banner_code,
 b.selling_channel,
 b.cc,
 b.banner_channel_cc,
 b.country_banner_channel_cc,
 b.cc_dsci,
 b.div_num,
 b.div_label,
 b.subdiv_num,
 b.subdiv_label,
 b.dept_num,
 b.dept_label,
 b.class_num,
 b.class_label,
 b.subclass_num,
 b.subclass_label,
 b.style_group_num,
 b.rms_style_num,
 b.color_num,
 b.vpn,
 b.supp_color,
 b.style_desc,
 b.prmy_supp_num,
 b.vendor_name,
 b.vendor_label_name,
 b.npg_ind,
 b.promotion_ind,
 b.anniversary_item_ind,
 b.replenishment_eligible_ind,
 b.drop_ship_eligible_ind,
 b.return_disposition_code,
 b.selling_status_code_legacy,
 b.first_rack_date,
 b.rack_ind,
 b.online_purchasable_ind,
 b.online_purchasable_eff_begin_date,
 b.online_purchasable_eff_end_date,
 b.average_unit_cost,
 b.price_base,
 b.price_ownership,
 b.price_ownership_future,
 b.price_selling,
 b.price_base_drop_ship,
 b.price_compare_at,
 b.price_current_rack,
 b.price_variance_within_cc_ind,
 b.price_base_next_highest_dollar,
 b.price_ownership_next_highest_dollar,
 b.price_selling_next_highest_dollar,
 b.price_ownership_future_next_highest_dollar,
 b.first_clearance_markdown_date,
 b.last_clearance_markdown_date,
 b.future_clearance_markdown_date,
 b.clearance_markdown_version,
 b.clearance_markdown_state,
 b.future_markdown_ind,
 b.weeks_since_last_markdown,
 b.pct_off_ownership_vs_base,
 b.pct_off_selling_vs_base,
 b.pct_off_ownership_future_vs_base,
 b.pct_off_ownership_vs_compare_at,
 b.pct_off_ownership_future_vs_compare_at,
 b.pct_off_band_ownership_vs_base,
 b.pct_off_band_selling_vs_base,
 b.pct_off_band_ownership_future_vs_base,
 b.pct_off_band_ownership_vs_compare_at,
 b.pct_off_band_ownership_future_vs_compare_at,
 b.price_type_code_ownership,
 b.price_type_code_ownership_future,
 b.price_type_code_selling,
 b.price_type_ownership,
 b.price_type_ownership_future,
 b.price_type_selling,
 b.price_signal,
 b.price_signal_future,
 b.markdown_type_ownership,
 b.markdown_type_ownership_future,
 b.lifecycle_phase_name_ownership,
 b.lifecycle_phase_name_ownership_future,
 b.sales_retail_1wk,
 b.sales_units_1wk,
 b.sales_retail_2wk,
 b.sales_units_2wk,
 b.sales_retail_4wk,
 b.sales_units_4wk,
 b.sales_retail_6mo,
 b.sales_units_6mo,
 b.sell_thru_4wk,
 b.sell_thru_since_last_markdown,
 b.sell_thru_4wk_avg_wkly,
 b.sell_thru_band_4wk_avg_wkly,
 b.sell_thru_since_last_markdown_avg_wkly,
 b.sell_thru_band_since_last_markdown_avg_wkly,
 b.first_receipt_date,
 b.last_receipt_date,
 b.weeks_since_last_receipt_date,
 b.weeks_since_last_receipt_date_band,
 b.weeks_available_to_sell,
 b.weeks_available_to_sell_band,
 b.first_sales_date,
 b.weeks_since_first_sale,
 b.weeks_since_first_sale_band,
 b.eop_units_selling_locations,
 b.eop_retail_selling_locations,
 b.eop_cost_selling_locations,
 b.eop_retail_future_selling_locations,
 b.eop_units_reserve_stock,
 b.eop_retail_reserve_stock,
 b.eop_cost_reserve_stock,
 b.eop_retail_future_reserve_stock,
 b.eop_units_pack_and_hold,
 b.eop_retail_pack_and_hold,
 b.eop_cost_pack_and_hold,
 b.eop_retail_future_pack_and_hold,
 b.eop_units_all_locations,
 b.eop_retail_all_locations,
 b.eop_cost_all_locations,
 b.eop_retail_future_all_locations,
 b.on_order_units_selling_locations,
 b.on_order_units_reserve_stock,
 b.on_order_units_pack_and_hold,
 b.on_order_units_all_locations,
 b.markdown_dollars_selling_locations,
 b.markdown_dollars_pack_and_hold,
 b.markdown_dollars_reserve_stock,
 b.markdown_dollars_all_locations,
 b.markdown_dollars_future_selling_locations,
 b.markdown_dollars_future_pack_and_hold,
 b.markdown_dollars_future_reserve_stock,
 b.markdown_dollars_future_all_locations,
 b.drop_ship_eligible_ind_other_banner,
 b.replenishment_eligible_ind_other_banner,
 b.price_type_ownership_other_banner,
 b.price_type_ownership_future_other_banner,
 b.price_type_selling_other_banner,
 b.price_base_other_banner,
 b.price_ownership_other_banner,
 b.price_ownership_future_other_banner,
 b.price_selling_other_banner,
 b.future_clearance_markdown_date_other_banner,
 b.future_markdown_ind_other_banner,
 b.sell_thru_4wk_other_banner,
 b.sell_thru_since_last_markdown_other_banner,
 b.sell_thru_4wk_avg_wkly_other_banner,
 b.sell_thru_band_4wk_avg_wkly_other_banner,
 b.sell_thru_since_last_markdown_avg_wkly_other_banner,
 b.sell_thru_band_since_last_markdown_avg_wkly_other_banner,
 b.eop_units_selling_locations_other_banner,
 b.eop_retail_selling_locations_other_banner,
 b.eop_cost_selling_locations_other_banner,
 b.eop_units_all_locations_other_banner,
 b.eop_retail_all_locations_other_banner,
 b.eop_cost_all_locations_other_banner,
 q.quantrix_category,
 q.quantrix_category_group,
 q.quantrix_season,
  CASE
  WHEN a.anchor_brand_name IS NOT NULL
  THEN 'Y'
  ELSE 'N'
  END AS anchor_brand_ind,
 s.selling_rights_ind,
 s.selling_status_code,
 ii.intent_season,
 ii.intent_lifecycle_type,
 ii.intent_scaled_event,
 ii.intent_holiday_or_celebration,
 ii.intent_exit_month_idnt,
 ii.intent_plan_week_idnt
FROM add_other_banner AS b
 LEFT JOIN category_lkup AS q ON b.dept_num = q.dept_num AND b.class_num = q.class_num AND b.subclass_num = q.sbclass_num
   
 LEFT JOIN anchor_brands_lkup AS a ON LOWER(a.banner_code) = LOWER(b.banner_code) AND b.dept_num = a.dept_num AND a.supplier_idnt
    = CAST(b.prmy_supp_num AS FLOAT64)
 LEFT JOIN selling_rights_stg AS s ON b.snapshot_date = s.snapshot_date AND LOWER(s.banner_channel_cc) = LOWER(b.banner_channel_cc
    )
 LEFT JOIN item_intent_lkup AS ii ON LOWER(ii.banner_channel_cc) = LOWER(b.banner_channel_cc);
--collect statistics    column ( snapshot_date, banner_channel_cc )    ,column (price_ownership_future_next_highest_dollar)    ,column (price_ownership_next_highest_dollar)    ,column (price_base_next_highest_dollar)    ,column (price_selling_next_highest_dollar) on add_nap_dims 
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
CREATE TEMPORARY TABLE IF NOT EXISTS price_band_lkup AS
SELECT *
FROM `{{params.gcp_project_id}}`.t2dl_das_in_season_management_reporting.price_bands_by_dollar;
--collect statistics    column(next_highest_dollar) on price_band_lkup 
-- Base Price
-- add zero before the decimal place
-- Ownership Price
-- if price = 0 then lower bound = 0; otherwise, fetch lower bound from the price band table
-- if price = 0 then upper bound = 0; otherwise, fetch upper bound from the price band table
-- assemble the price band
-- first character will be a dollar sign
-- add a zero before the decimal place if price < $1.00
-- lower bound of price range
-- if price is zero there is no upper bound to the price range
-- if upper bound is the highest possible upper bound, show a + instead of the actual value 
-- if upper bound is not the highest possible upper bound, show the upper bound
-- Ownership Price Future
-- add zero before the decimal place
-- Selling Price
-- add zero before the decimal place
CREATE TEMPORARY TABLE IF NOT EXISTS add_price_bands AS
SELECT b.snapshot_date,
 b.channel_country,
 b.channel_brand,
 b.banner_code,
 b.selling_channel,
 b.cc,
 b.banner_channel_cc,
 b.country_banner_channel_cc,
 b.cc_dsci,
 b.div_num,
 b.div_label,
 b.subdiv_num,
 b.subdiv_label,
 b.dept_num,
 b.dept_label,
 b.class_num,
 b.class_label,
 b.subclass_num,
 b.subclass_label,
 b.style_group_num,
 b.rms_style_num,
 b.color_num,
 b.vpn,
 b.supp_color,
 b.style_desc,
 b.prmy_supp_num,
 b.vendor_name,
 b.vendor_label_name,
 b.npg_ind,
 b.promotion_ind,
 b.anniversary_item_ind,
 b.replenishment_eligible_ind,
 b.drop_ship_eligible_ind,
 b.return_disposition_code,
 b.selling_status_code_legacy,
 b.first_rack_date,
 b.rack_ind,
 b.online_purchasable_ind,
 b.online_purchasable_eff_begin_date,
 b.online_purchasable_eff_end_date,
 b.average_unit_cost,
 b.price_base,
 b.price_ownership,
 b.price_ownership_future,
 b.price_selling,
 b.price_base_drop_ship,
 b.price_compare_at,
 b.price_current_rack,
 b.price_variance_within_cc_ind,
 b.price_base_next_highest_dollar,
 b.price_ownership_next_highest_dollar,
 b.price_selling_next_highest_dollar,
 b.price_ownership_future_next_highest_dollar,
 b.first_clearance_markdown_date,
 b.last_clearance_markdown_date,
 b.future_clearance_markdown_date,
 b.clearance_markdown_version,
 b.clearance_markdown_state,
 b.future_markdown_ind,
 b.weeks_since_last_markdown,
 b.pct_off_ownership_vs_base,
 b.pct_off_selling_vs_base,
 b.pct_off_ownership_future_vs_base,
 b.pct_off_ownership_vs_compare_at,
 b.pct_off_ownership_future_vs_compare_at,
 b.pct_off_band_ownership_vs_base,
 b.pct_off_band_selling_vs_base,
 b.pct_off_band_ownership_future_vs_base,
 b.pct_off_band_ownership_vs_compare_at,
 b.pct_off_band_ownership_future_vs_compare_at,
 b.price_type_code_ownership,
 b.price_type_code_ownership_future,
 b.price_type_code_selling,
 b.price_type_ownership,
 b.price_type_ownership_future,
 b.price_type_selling,
 b.price_signal,
 b.price_signal_future,
 b.markdown_type_ownership,
 b.markdown_type_ownership_future,
 b.lifecycle_phase_name_ownership,
 b.lifecycle_phase_name_ownership_future,
 b.sales_retail_1wk,
 b.sales_units_1wk,
 b.sales_retail_2wk,
 b.sales_units_2wk,
 b.sales_retail_4wk,
 b.sales_units_4wk,
 b.sales_retail_6mo,
 b.sales_units_6mo,
 b.sell_thru_4wk,
 b.sell_thru_since_last_markdown,
 b.sell_thru_4wk_avg_wkly,
 b.sell_thru_band_4wk_avg_wkly,
 b.sell_thru_since_last_markdown_avg_wkly,
 b.sell_thru_band_since_last_markdown_avg_wkly,
 b.first_receipt_date,
 b.last_receipt_date,
 b.weeks_since_last_receipt_date,
 b.weeks_since_last_receipt_date_band,
 b.weeks_available_to_sell,
 b.weeks_available_to_sell_band,
 b.first_sales_date,
 b.weeks_since_first_sale,
 b.weeks_since_first_sale_band,
 b.eop_units_selling_locations,
 b.eop_retail_selling_locations,
 b.eop_cost_selling_locations,
 b.eop_retail_future_selling_locations,
 b.eop_units_reserve_stock,
 b.eop_retail_reserve_stock,
 b.eop_cost_reserve_stock,
 b.eop_retail_future_reserve_stock,
 b.eop_units_pack_and_hold,
 b.eop_retail_pack_and_hold,
 b.eop_cost_pack_and_hold,
 b.eop_retail_future_pack_and_hold,
 b.eop_units_all_locations,
 b.eop_retail_all_locations,
 b.eop_cost_all_locations,
 b.eop_retail_future_all_locations,
 b.on_order_units_selling_locations,
 b.on_order_units_reserve_stock,
 b.on_order_units_pack_and_hold,
 b.on_order_units_all_locations,
 b.markdown_dollars_selling_locations,
 b.markdown_dollars_pack_and_hold,
 b.markdown_dollars_reserve_stock,
 b.markdown_dollars_all_locations,
 b.markdown_dollars_future_selling_locations,
 b.markdown_dollars_future_pack_and_hold,
 b.markdown_dollars_future_reserve_stock,
 b.markdown_dollars_future_all_locations,
 b.drop_ship_eligible_ind_other_banner,
 b.replenishment_eligible_ind_other_banner,
 b.price_type_ownership_other_banner,
 b.price_type_ownership_future_other_banner,
 b.price_type_selling_other_banner,
 b.price_base_other_banner,
 b.price_ownership_other_banner,
 b.price_ownership_future_other_banner,
 b.price_selling_other_banner,
 b.future_clearance_markdown_date_other_banner,
 b.future_markdown_ind_other_banner,
 b.sell_thru_4wk_other_banner,
 b.sell_thru_since_last_markdown_other_banner,
 b.sell_thru_4wk_avg_wkly_other_banner,
 b.sell_thru_band_4wk_avg_wkly_other_banner,
 b.sell_thru_since_last_markdown_avg_wkly_other_banner,
 b.sell_thru_band_since_last_markdown_avg_wkly_other_banner,
 b.eop_units_selling_locations_other_banner,
 b.eop_retail_selling_locations_other_banner,
 b.eop_cost_selling_locations_other_banner,
 b.eop_units_all_locations_other_banner,
 b.eop_retail_all_locations_other_banner,
 b.eop_cost_all_locations_other_banner,
 b.quantrix_category,
 b.quantrix_category_group,
 b.quantrix_season,
 b.anchor_brand_ind,
 b.selling_rights_ind,
 b.selling_status_code,
 b.intent_season,
 b.intent_lifecycle_type,
 b.intent_scaled_event,
 b.intent_holiday_or_celebration,
 b.intent_exit_month_idnt,
 b.intent_plan_week_idnt,
  CASE
  WHEN COALESCE(b.price_base, 0) = 0
  THEN 0
  ELSE COALESCE(pbb.price_band_two_lower_bound, (SELECT MAX(price_band_two_lower_bound)
    FROM price_band_lkup))
  END AS pbb_lower_bound,
  CASE
  WHEN COALESCE(b.price_base, 0) = 0
  THEN 0
  ELSE COALESCE(pbb.price_band_two_upper_bound, (SELECT MAX(price_band_two_upper_bound)
    FROM price_band_lkup))
  END AS pbb_upper_bound,
    '$' || CASE
     WHEN CASE
       WHEN COALESCE(b.price_base, 0) = 0
       THEN 0
       ELSE COALESCE(pbb.price_band_two_lower_bound, (SELECT MAX(price_band_two_lower_bound)
         FROM price_band_lkup))
       END < 1
     THEN '0'
     ELSE ''
     END || TRIM(FORMAT('%39.0f.', CASE
      WHEN COALESCE(b.price_base, 0) = 0
      THEN 0
      ELSE COALESCE(pbb.price_band_two_lower_bound, (SELECT MAX(price_band_two_lower_bound)
        FROM price_band_lkup))
      END)) || CASE
   WHEN CASE
     WHEN COALESCE(b.price_base, 0) = 0
     THEN 0
     ELSE COALESCE(pbb.price_band_two_lower_bound, (SELECT MAX(price_band_two_lower_bound)
       FROM price_band_lkup))
     END = 0
   THEN ''
   ELSE CASE
    WHEN CASE
      WHEN COALESCE(b.price_base, 0) = 0
      THEN 0
      ELSE COALESCE(pbb.price_band_two_upper_bound, (SELECT MAX(price_band_two_upper_bound)
        FROM price_band_lkup))
      END = (SELECT MAX(price_band_two_upper_bound)
      FROM price_band_lkup)
    THEN '+'
    ELSE ' - ' || TRIM(FORMAT('%39.0f.', CASE
        WHEN COALESCE(b.price_base, 0) = 0
        THEN 0
        ELSE COALESCE(pbb.price_band_two_upper_bound, (SELECT MAX(price_band_two_upper_bound)
          FROM price_band_lkup))
        END))
    END
   END AS price_band_base,
  CASE
  WHEN COALESCE(b.price_ownership, 0) = 0
  THEN 0
  ELSE COALESCE(pbo.price_band_two_lower_bound, (SELECT MAX(price_band_two_lower_bound)
    FROM price_band_lkup))
  END AS pbo_lower_bound,
  CASE
  WHEN COALESCE(b.price_ownership, 0) = 0
  THEN 0
  ELSE COALESCE(pbo.price_band_two_upper_bound, (SELECT MAX(price_band_two_upper_bound)
    FROM price_band_lkup))
  END AS pbo_upper_bound,
    '$' || CASE
     WHEN CASE
       WHEN COALESCE(b.price_ownership, 0) = 0
       THEN 0
       ELSE COALESCE(pbo.price_band_two_lower_bound, (SELECT MAX(price_band_two_lower_bound)
         FROM price_band_lkup))
       END < 1
     THEN '0'
     ELSE ''
     END || TRIM(FORMAT('%39.0f.', CASE
      WHEN COALESCE(b.price_ownership, 0) = 0
      THEN 0
      ELSE COALESCE(pbo.price_band_two_lower_bound, (SELECT MAX(price_band_two_lower_bound)
        FROM price_band_lkup))
      END)) || CASE
   WHEN CASE
     WHEN COALESCE(b.price_ownership, 0) = 0
     THEN 0
     ELSE COALESCE(pbo.price_band_two_lower_bound, (SELECT MAX(price_band_two_lower_bound)
       FROM price_band_lkup))
     END = 0
   THEN ''
   ELSE CASE
    WHEN CASE
      WHEN COALESCE(b.price_ownership, 0) = 0
      THEN 0
      ELSE COALESCE(pbo.price_band_two_upper_bound, (SELECT MAX(price_band_two_upper_bound)
        FROM price_band_lkup))
      END = (SELECT MAX(price_band_two_upper_bound)
      FROM price_band_lkup)
    THEN '+'
    ELSE ' - ' || TRIM(FORMAT('%39.0f.', CASE
        WHEN COALESCE(b.price_ownership, 0) = 0
        THEN 0
        ELSE COALESCE(pbo.price_band_two_upper_bound, (SELECT MAX(price_band_two_upper_bound)
          FROM price_band_lkup))
        END))
    END
   END AS price_band_ownership,
  CASE
  WHEN COALESCE(b.price_ownership, 0) = 0
  THEN 0
  ELSE COALESCE(pbof.price_band_two_lower_bound, (SELECT MAX(price_band_two_lower_bound)
    FROM price_band_lkup))
  END AS pbof_lower_bound,
  CASE
  WHEN COALESCE(b.price_ownership, 0) = 0
  THEN 0
  ELSE COALESCE(pbof.price_band_two_upper_bound, (SELECT MAX(price_band_two_upper_bound)
    FROM price_band_lkup))
  END AS pbof_upper_bound,
    '$' || CASE
     WHEN CASE
       WHEN COALESCE(b.price_ownership, 0) = 0
       THEN 0
       ELSE COALESCE(pbof.price_band_two_lower_bound, (SELECT MAX(price_band_two_lower_bound)
         FROM price_band_lkup))
       END < 1
     THEN '0'
     ELSE ''
     END || TRIM(FORMAT('%39.0f.', CASE
      WHEN COALESCE(b.price_ownership, 0) = 0
      THEN 0
      ELSE COALESCE(pbof.price_band_two_lower_bound, (SELECT MAX(price_band_two_lower_bound)
        FROM price_band_lkup))
      END)) || CASE
   WHEN CASE
     WHEN COALESCE(b.price_ownership, 0) = 0
     THEN 0
     ELSE COALESCE(pbof.price_band_two_lower_bound, (SELECT MAX(price_band_two_lower_bound)
       FROM price_band_lkup))
     END = 0
   THEN ''
   ELSE CASE
    WHEN CASE
      WHEN COALESCE(b.price_ownership, 0) = 0
      THEN 0
      ELSE COALESCE(pbof.price_band_two_upper_bound, (SELECT MAX(price_band_two_upper_bound)
        FROM price_band_lkup))
      END = (SELECT MAX(price_band_two_upper_bound)
      FROM price_band_lkup)
    THEN '+'
    ELSE ' - ' || TRIM(FORMAT('%39.0f.', CASE
        WHEN COALESCE(b.price_ownership, 0) = 0
        THEN 0
        ELSE COALESCE(pbof.price_band_two_upper_bound, (SELECT MAX(price_band_two_upper_bound)
          FROM price_band_lkup))
        END))
    END
   END AS price_band_ownership_future,
  CASE
  WHEN COALESCE(b.price_selling, 0) = 0
  THEN 0
  ELSE COALESCE(pbs.price_band_two_lower_bound, (SELECT MAX(price_band_two_lower_bound)
    FROM price_band_lkup))
  END AS pbs_lower_bound,
  CASE
  WHEN COALESCE(b.price_selling, 0) = 0
  THEN 0
  ELSE COALESCE(pbs.price_band_two_upper_bound, (SELECT MAX(price_band_two_upper_bound)
    FROM price_band_lkup))
  END AS pbs_upper_bound,
    '$' || CASE
     WHEN CASE
       WHEN COALESCE(b.price_selling, 0) = 0
       THEN 0
       ELSE COALESCE(pbs.price_band_two_lower_bound, (SELECT MAX(price_band_two_lower_bound)
         FROM price_band_lkup))
       END < 1
     THEN '0'
     ELSE ''
     END || TRIM(FORMAT('%39.0f.', CASE
      WHEN COALESCE(b.price_selling, 0) = 0
      THEN 0
      ELSE COALESCE(pbs.price_band_two_lower_bound, (SELECT MAX(price_band_two_lower_bound)
        FROM price_band_lkup))
      END)) || CASE
   WHEN CASE
     WHEN COALESCE(b.price_selling, 0) = 0
     THEN 0
     ELSE COALESCE(pbs.price_band_two_lower_bound, (SELECT MAX(price_band_two_lower_bound)
       FROM price_band_lkup))
     END = 0
   THEN ''
   ELSE CASE
    WHEN CASE
      WHEN COALESCE(b.price_selling, 0) = 0
      THEN 0
      ELSE COALESCE(pbs.price_band_two_upper_bound, (SELECT MAX(price_band_two_upper_bound)
        FROM price_band_lkup))
      END = (SELECT MAX(price_band_two_upper_bound)
      FROM price_band_lkup)
    THEN '+'
    ELSE ' - ' || TRIM(FORMAT('%39.0f.', CASE
        WHEN COALESCE(b.price_selling, 0) = 0
        THEN 0
        ELSE COALESCE(pbs.price_band_two_upper_bound, (SELECT MAX(price_band_two_upper_bound)
          FROM price_band_lkup))
        END))
    END
   END AS price_band_selling
FROM add_nap_dims AS b
 LEFT JOIN price_band_lkup AS pbb ON b.price_base_next_highest_dollar = pbb.next_highest_dollar
 LEFT JOIN price_band_lkup AS pbo ON b.price_ownership_next_highest_dollar = pbo.next_highest_dollar
 LEFT JOIN price_band_lkup AS pbof ON b.price_ownership_future_next_highest_dollar = pbof.next_highest_dollar
 LEFT JOIN price_band_lkup AS pbs ON b.price_selling_next_highest_dollar = pbs.next_highest_dollar;
--collect statistics    column( snapshot_date, banner_channel_cc ) on add_price_bands 
/*
 * GET SPECIFIC FIELDS FROM PRIOR WEEK
 * 
 * Reporting: These are used only in the Last Chance tab in the Markdown Recap Report.
 * Operations: Other calculations are needed to identify Net New Marks. Those are not included here.
 */
-- choose the week prior to the most recent week
CREATE TEMPORARY TABLE IF NOT EXISTS prior_week_lkup AS
SELECT DATE_ADD(snapshot_date, INTERVAL 7 DAY) AS snapshot_date_for_reporting_week,
 snapshot_date AS snapshot_date_for_prior_week,
  CASE
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM')
  THEN 'N'
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
  THEN 'R'
  ELSE NULL
  END AS banner_code,
   rms_style_num || '-' || color_num AS cc,
      CASE
      WHEN LOWER(channel_brand) = LOWER('NORDSTROM')
      THEN 'N'
      WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
      THEN 'R'
      ELSE NULL
      END || '-' || selling_channel || '-' || (rms_style_num || '-' || color_num) AS banner_channel_cc,
 future_clearance_markdown_date AS future_clearance_markdown_date_prior_week,
 future_clearance_markdown_price AS price_ownership_future_prior_week,
 ownership_price_amt AS price_ownership_prior_week,
 total_inv_qty AS eop_units_selling_locations_prior_week
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.clearance_markdown_insights_by_week_fact
WHERE snapshot_date = DATE_SUB((SELECT snapshot_date
    FROM snap_date), INTERVAL 7 DAY);
--collect statistics    column( snapshot_date_for_reporting_week, banner_channel_cc ) on prior_week_lkup 
CREATE TEMPORARY TABLE IF NOT EXISTS add_prior_week AS
SELECT pb.snapshot_date,
 pb.channel_country,
 pb.channel_brand,
 pb.banner_code,
 pb.selling_channel,
 pb.cc,
 pb.banner_channel_cc,
 pb.country_banner_channel_cc,
 pb.cc_dsci,
 pb.div_num,
 pb.div_label,
 pb.subdiv_num,
 pb.subdiv_label,
 pb.dept_num,
 pb.dept_label,
 pb.class_num,
 pb.class_label,
 pb.subclass_num,
 pb.subclass_label,
 pb.style_group_num,
 pb.rms_style_num,
 pb.color_num,
 pb.vpn,
 pb.supp_color,
 pb.style_desc,
 pb.prmy_supp_num,
 pb.vendor_name,
 pb.vendor_label_name,
 pb.npg_ind,
 pb.promotion_ind,
 pb.anniversary_item_ind,
 pb.replenishment_eligible_ind,
 pb.drop_ship_eligible_ind,
 pb.return_disposition_code,
 pb.selling_status_code_legacy,
 pb.first_rack_date,
 pb.rack_ind,
 pb.online_purchasable_ind,
 pb.online_purchasable_eff_begin_date,
 pb.online_purchasable_eff_end_date,
 pb.average_unit_cost,
 pb.price_base,
 pb.price_ownership,
 pb.price_ownership_future,
 pb.price_selling,
 pb.price_base_drop_ship,
 pb.price_compare_at,
 pb.price_current_rack,
 pb.price_variance_within_cc_ind,
 pb.price_base_next_highest_dollar,
 pb.price_ownership_next_highest_dollar,
 pb.price_selling_next_highest_dollar,
 pb.price_ownership_future_next_highest_dollar,
 pb.first_clearance_markdown_date,
 pb.last_clearance_markdown_date,
 pb.future_clearance_markdown_date,
 pb.clearance_markdown_version,
 pb.clearance_markdown_state,
 pb.future_markdown_ind,
 pb.weeks_since_last_markdown,
 pb.pct_off_ownership_vs_base,
 pb.pct_off_selling_vs_base,
 pb.pct_off_ownership_future_vs_base,
 pb.pct_off_ownership_vs_compare_at,
 pb.pct_off_ownership_future_vs_compare_at,
 pb.pct_off_band_ownership_vs_base,
 pb.pct_off_band_selling_vs_base,
 pb.pct_off_band_ownership_future_vs_base,
 pb.pct_off_band_ownership_vs_compare_at,
 pb.pct_off_band_ownership_future_vs_compare_at,
 pb.price_type_code_ownership,
 pb.price_type_code_ownership_future,
 pb.price_type_code_selling,
 pb.price_type_ownership,
 pb.price_type_ownership_future,
 pb.price_type_selling,
 pb.price_signal,
 pb.price_signal_future,
 pb.markdown_type_ownership,
 pb.markdown_type_ownership_future,
 pb.lifecycle_phase_name_ownership,
 pb.lifecycle_phase_name_ownership_future,
 pb.sales_retail_1wk,
 pb.sales_units_1wk,
 pb.sales_retail_2wk,
 pb.sales_units_2wk,
 pb.sales_retail_4wk,
 pb.sales_units_4wk,
 pb.sales_retail_6mo,
 pb.sales_units_6mo,
 pb.sell_thru_4wk,
 pb.sell_thru_since_last_markdown,
 pb.sell_thru_4wk_avg_wkly,
 pb.sell_thru_band_4wk_avg_wkly,
 pb.sell_thru_since_last_markdown_avg_wkly,
 pb.sell_thru_band_since_last_markdown_avg_wkly,
 pb.first_receipt_date,
 pb.last_receipt_date,
 pb.weeks_since_last_receipt_date,
 pb.weeks_since_last_receipt_date_band,
 pb.weeks_available_to_sell,
 pb.weeks_available_to_sell_band,
 pb.first_sales_date,
 pb.weeks_since_first_sale,
 pb.weeks_since_first_sale_band,
 pb.eop_units_selling_locations,
 pb.eop_retail_selling_locations,
 pb.eop_cost_selling_locations,
 pb.eop_retail_future_selling_locations,
 pb.eop_units_reserve_stock,
 pb.eop_retail_reserve_stock,
 pb.eop_cost_reserve_stock,
 pb.eop_retail_future_reserve_stock,
 pb.eop_units_pack_and_hold,
 pb.eop_retail_pack_and_hold,
 pb.eop_cost_pack_and_hold,
 pb.eop_retail_future_pack_and_hold,
 pb.eop_units_all_locations,
 pb.eop_retail_all_locations,
 pb.eop_cost_all_locations,
 pb.eop_retail_future_all_locations,
 pb.on_order_units_selling_locations,
 pb.on_order_units_reserve_stock,
 pb.on_order_units_pack_and_hold,
 pb.on_order_units_all_locations,
 pb.markdown_dollars_selling_locations,
 pb.markdown_dollars_pack_and_hold,
 pb.markdown_dollars_reserve_stock,
 pb.markdown_dollars_all_locations,
 pb.markdown_dollars_future_selling_locations,
 pb.markdown_dollars_future_pack_and_hold,
 pb.markdown_dollars_future_reserve_stock,
 pb.markdown_dollars_future_all_locations,
 pb.drop_ship_eligible_ind_other_banner,
 pb.replenishment_eligible_ind_other_banner,
 pb.price_type_ownership_other_banner,
 pb.price_type_ownership_future_other_banner,
 pb.price_type_selling_other_banner,
 pb.price_base_other_banner,
 pb.price_ownership_other_banner,
 pb.price_ownership_future_other_banner,
 pb.price_selling_other_banner,
 pb.future_clearance_markdown_date_other_banner,
 pb.future_markdown_ind_other_banner,
 pb.sell_thru_4wk_other_banner,
 pb.sell_thru_since_last_markdown_other_banner,
 pb.sell_thru_4wk_avg_wkly_other_banner,
 pb.sell_thru_band_4wk_avg_wkly_other_banner,
 pb.sell_thru_since_last_markdown_avg_wkly_other_banner,
 pb.sell_thru_band_since_last_markdown_avg_wkly_other_banner,
 pb.eop_units_selling_locations_other_banner,
 pb.eop_retail_selling_locations_other_banner,
 pb.eop_cost_selling_locations_other_banner,
 pb.eop_units_all_locations_other_banner,
 pb.eop_retail_all_locations_other_banner,
 pb.eop_cost_all_locations_other_banner,
 pb.quantrix_category,
 pb.quantrix_category_group,
 pb.quantrix_season,
 pb.anchor_brand_ind,
 pb.selling_rights_ind,
 pb.selling_status_code,
 pb.intent_season,
 pb.intent_lifecycle_type,
 pb.intent_scaled_event,
 pb.intent_holiday_or_celebration,
 pb.intent_exit_month_idnt,
 pb.intent_plan_week_idnt,
 pb.pbb_lower_bound,
 pb.pbb_upper_bound,
 pb.price_band_base,
 pb.pbo_lower_bound,
 pb.pbo_upper_bound,
 pb.price_band_ownership,
 pb.pbof_lower_bound,
 pb.pbof_upper_bound,
 pb.price_band_ownership_future,
 pb.pbs_lower_bound,
 pb.pbs_upper_bound,
 pb.price_band_selling,
 pw.future_clearance_markdown_date_prior_week,
 pw.price_ownership_future_prior_week,
 pw.price_ownership_prior_week,
 pw.eop_units_selling_locations_prior_week
FROM add_price_bands AS pb
 LEFT JOIN prior_week_lkup AS pw ON pb.snapshot_date = pw.snapshot_date_for_reporting_week AND LOWER(pw.banner_channel_cc
    ) = LOWER(pb.banner_channel_cc);
/*
 * FULL DATA SET - FINAL
 * 
 * Reorder fields into related groups
 */
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.markdowns_analysis_single_week;
-- record ID and key dimensions
-- Merch Hierarchy
-- Product Attributes
-- Product Statuses
-- Cost
-- Price Amounts, Price Bands
-- Percent Off
-- Price Type, Price Signals
-- Mark Type, Lifecycle Phase
-- Markdown Info
-- Time in Assortment
-- Item Intent
-- Sales
-- Sell-Thru
-- Inventory
-- On Order
-- Markdown Dollars
-- Opposite Banner Info
-- Prior Week Info
-- Load Time
INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.markdowns_analysis_single_week
(SELECT snapshot_date,
  channel_country,
  channel_brand,
  banner_code,
  selling_channel,
  cc,
  banner_channel_cc,
  country_banner_channel_cc,
  cc_dsci,
  div_num,
  div_label,
  subdiv_num,
  subdiv_label,
  dept_num,
  dept_label,
  class_num,
  class_label,
  subclass_num,
  subclass_label,
  style_group_num,
  rms_style_num,
  color_num,
  vpn,
  supp_color,
  style_desc,
  prmy_supp_num,
  vendor_name,
  vendor_label_name,
  npg_ind,
  quantrix_category,
  quantrix_category_group,
  SUBSTR(quantrix_season, 1, 4) AS quantrix_season,
  anchor_brand_ind,
  promotion_ind,
  anniversary_item_ind,
  replenishment_eligible_ind,
  drop_ship_eligible_ind,
  return_disposition_code,
  first_rack_date,
  rack_ind,
  online_purchasable_ind,
  online_purchasable_eff_begin_date,
  online_purchasable_eff_end_date,
  selling_status_code_legacy,
  selling_status_code,
  selling_rights_ind,
  ROUND(CAST(ROUND(average_unit_cost, 2) AS NUMERIC), 2) AS average_unit_cost,
  price_base,
  price_ownership,
  price_ownership_future,
  price_selling,
  price_band_base,
  price_band_ownership,
  price_band_ownership_future,
  price_band_selling,
  price_base_drop_ship,
  price_compare_at,
  price_current_rack,
  price_variance_within_cc_ind,
  CAST(pct_off_ownership_vs_base AS NUMERIC) AS pct_off_ownership_vs_base,
  CAST(pct_off_selling_vs_base AS NUMERIC) AS pct_off_selling_vs_base,
  CAST(pct_off_ownership_future_vs_base AS NUMERIC) AS pct_off_ownership_future_vs_base,
  CAST(pct_off_ownership_vs_compare_at AS NUMERIC) AS pct_off_ownership_vs_compare_at,
  CAST(pct_off_ownership_future_vs_compare_at AS NUMERIC) AS pct_off_ownership_future_vs_compare_at,
  pct_off_band_ownership_vs_base,
  pct_off_band_selling_vs_base,
  pct_off_band_ownership_future_vs_base,
  pct_off_band_ownership_vs_compare_at,
  pct_off_band_ownership_future_vs_compare_at,
  price_type_code_ownership,
  price_type_code_ownership_future,
  price_type_code_selling,
  price_type_ownership,
  price_type_ownership_future,
  price_type_selling,
  price_signal,
  price_signal_future,
  markdown_type_ownership,
  markdown_type_ownership_future,
  lifecycle_phase_name_ownership,
  lifecycle_phase_name_ownership_future,
  first_clearance_markdown_date,
  last_clearance_markdown_date,
  future_clearance_markdown_date,
  clearance_markdown_version,
  future_markdown_ind,
  weeks_since_last_markdown,
  first_receipt_date,
  last_receipt_date,
  cast(TRUNC(weeks_since_last_receipt_date) as int64),
  cast(weeks_since_last_receipt_date_band as string),
  cast(TRUNC(weeks_available_to_sell) as int64),
  weeks_available_to_sell_band,
  first_sales_date,
  cast(TRUNC(weeks_since_first_sale) as int64),
  weeks_since_first_sale_band,
  intent_plan_week_idnt,
  intent_exit_month_idnt,
  intent_season,
  intent_lifecycle_type,
  intent_scaled_event,
  intent_holiday_or_celebration,
  sales_retail_1wk,
  CAST(sales_units_1wk AS NUMERIC) AS sales_units_1wk,
  sales_retail_2wk,
  CAST(sales_units_2wk AS NUMERIC) AS sales_units_2wk,
  sales_retail_4wk,
  CAST(sales_units_4wk AS NUMERIC) AS sales_units_4wk,
  sales_retail_6mo,
  CAST(sales_units_6mo AS NUMERIC) AS sales_units_6mo,
  sell_thru_4wk,
  sell_thru_since_last_markdown,
  sell_thru_4wk_avg_wkly,
  sell_thru_band_4wk_avg_wkly,
  CAST(sell_thru_since_last_markdown_avg_wkly AS NUMERIC) AS sell_thru_since_last_markdown_avg_wkly,
  sell_thru_band_since_last_markdown_avg_wkly,
  eop_units_selling_locations,
  eop_retail_selling_locations,
  eop_cost_selling_locations,
  eop_retail_future_selling_locations,
  eop_units_reserve_stock,
  eop_retail_reserve_stock,
  eop_cost_reserve_stock,
  eop_retail_future_reserve_stock,
  eop_units_pack_and_hold,
  eop_retail_pack_and_hold,
  eop_cost_pack_and_hold,
  eop_retail_future_pack_and_hold,
  eop_units_all_locations,
  eop_retail_all_locations,
  eop_cost_all_locations,
  eop_retail_future_all_locations,
  on_order_units_selling_locations,
  on_order_units_reserve_stock,
  on_order_units_pack_and_hold,
  on_order_units_all_locations,
  markdown_dollars_selling_locations,
  markdown_dollars_reserve_stock,
  markdown_dollars_pack_and_hold,
  markdown_dollars_all_locations,
  markdown_dollars_future_selling_locations,
  markdown_dollars_future_reserve_stock,
  markdown_dollars_future_pack_and_hold,
  markdown_dollars_future_all_locations,
  drop_ship_eligible_ind_other_banner,
  replenishment_eligible_ind_other_banner,
  price_type_ownership_other_banner,
  price_type_ownership_future_other_banner,
  price_type_selling_other_banner,
  price_base_other_banner,
  price_ownership_other_banner,
  price_ownership_future_other_banner,
  price_selling_other_banner,
  future_clearance_markdown_date_other_banner,
  future_markdown_ind_other_banner,
  sell_thru_4wk_other_banner,
  sell_thru_since_last_markdown_other_banner,
  sell_thru_4wk_avg_wkly_other_banner,
  sell_thru_band_4wk_avg_wkly_other_banner,
  CAST(sell_thru_since_last_markdown_avg_wkly_other_banner AS NUMERIC) AS
  sell_thru_since_last_markdown_avg_wkly_other_banner,
  sell_thru_band_since_last_markdown_avg_wkly_other_banner,
  eop_units_selling_locations_other_banner,
  eop_retail_selling_locations_other_banner,
  eop_cost_selling_locations_other_banner,
  eop_units_all_locations_other_banner,
  eop_retail_all_locations_other_banner,
  eop_cost_all_locations_other_banner,
  future_clearance_markdown_date_prior_week,
  price_ownership_future_prior_week,
  price_ownership_prior_week,
  eop_units_selling_locations_prior_week,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP) AS
  dw_sys_load_tmstp
 FROM add_prior_week AS apw);
-- stats to collect are defined in DDL
--collect statistics on t2dl_das_in_season_management_reporting.markdowns_analysis_single_week 