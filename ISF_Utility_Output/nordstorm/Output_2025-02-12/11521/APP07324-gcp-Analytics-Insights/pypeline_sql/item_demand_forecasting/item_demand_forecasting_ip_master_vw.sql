/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08073;
     DAG_ID=item_demand_forecasting_ip_master_view_11521_ACE_ENG;
     Task_Name=create_ip_master_view;'
     FOR SESSION VOLATILE;


/*

T2/View Name: IDF_MASTER_ONLINE_VW
Team/Owner: Item Demand Forecasting Team (SCDS)
Date Created/Modified: 2024-03

Note:
This table pulls together all of the idf feature tables into one master using online sales as a base 
The product view is merged into this table to get the most up to date product information
This view only contains the online demand by CC x Week

*/
REPLACE VIEW {ip_forecast_t2_schema}.IP_MASTER_ONLINE_VW AS LOCK ROW FOR ACCESS
with sales as (
select
  channel_brand,
  selling_channel,
  epm_choice_num,
  dma_cd,
  week_start_date,
  week_num,
  sum(order_line_quantity) as order_line_quantity,
  sum(bopus_order_line_quantity) as bopus_order_line_quantity,
  sum(dropship_order_line_quantity) as dropship_order_line_quantity,
  sum(fc_fill_order_line_quantity) as fc_fill_order_line_quantity
from
  {ip_forecast_t2_schema}.idf_online_sale
where 
  selling_channel = 'ONLINE'
group by 
  channel_brand,
  selling_channel,
  epm_choice_num,
  dma_cd,
  week_start_date,
  week_num
)
select
  sales.week_num,
  sales.week_start_date,
  sales.channel_brand,
  sales.selling_channel,
  prod.dept_num,
  sales.epm_choice_num,
  sales.dma_cd,
  sales.order_line_quantity as demand,
  sales.bopus_order_line_quantity as demand_store_take,
  sales.order_line_quantity - sales.bopus_order_line_quantity as demand_non_bopus,
  sales.order_line_quantity - sales.dropship_order_line_quantity as demand_owned,
  sales.dropship_order_line_quantity as demand_dropship,
  sales.fc_fill_order_line_quantity as demand_fc_filled,
  prod.class_num,
  prod.sbclass_num,
  prod.brand_name,
  prod.prmy_supp_num,
  prod.manufacturer_num,
  prod.color_num,
  prod.color_desc,
  prod.epm_style_num,
  prod.style_desc,
  prod.live_date,
  coalesce(rp.rp_ind, 0) as rp_ind
from
  sales
join 
  {ip_forecast_t2_schema}.idf_product_view prod
  on sales.epm_choice_num = prod.epm_choice_num
left join
  {ip_forecast_t2_schema}.idf_rp rp
  on sales.epm_choice_num = rp.epm_choice_num
  and sales.week_start_date= rp.week_start_date
  and sales.selling_channel = rp.selling_channel
  and sales.channel_brand = rp.channel_brand;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;