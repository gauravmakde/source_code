/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08073;
     DAG_ID=item_demand_forecasting_master_view_11521_ACE_ENG;
     Task_Name=create_master_view;'
     FOR SESSION VOLATILE;


/*

T2/View Name: IDF_MASTER_ONLINE_VW
Team/Owner: Item Demand Forecasting Team (SCDS)
Date Created/Modified: 2024-02

Note:
This table pulls together all of the idf feature tables into one master using online inventory and online sales as a base 
The product view is merged into this table to get the most up to date product information
This view only contains the online demand by CC x Week

*/
REPLACE VIEW {ip_forecast_t2_schema}.IDF_MASTER_ONLINE_VW AS LOCK ROW FOR ACCESS
  SELECT distinct 
    inv.week_num,
    inv.week_start_date, 
    inv.selling_channel,
    prod.dept_num,
    inv.channel_brand,
    inv.epm_choice_num,
    inv.regular_price_amt,
    inv.boh,
    inv.boh_sku_ct,
    inv.boh_store,
    inv.boh_store_sku_ct,
    inv.boh_store_ct,
    sales.order_line_quantity as demand,
    sales.bopus_order_line_quantity as demand_store_take,
    sales.order_line_quantity - sales.dropship_order_line_quantity as demand_owned,
    sales.dropship_order_line_quantity as demand_dropship,
    sales_hist.order_line_quantity as demand_hist,
    sales_hist.bopus_order_line_quantity as demand_store_take_hist,
    sales_hist.order_line_quantity - sales_hist.dropship_order_line_quantity as demand_owned_hist,
    sales_hist.dropship_order_line_quantity as demand_dropship_hist,
    sm.merch_gross_sales,
    sm.merch_regular_gross_sales,
    drop_inv.dropship_boh,
    drop_inv.dropship_sku_ct,
    digital.averagerating,
    digital.product_views,
    digital.add_to_bag,
    price.current_price_amt,
    price.current_price_type,
    price_event.current_price_event,
	  price_event.event_tags,
    rp.rp_ind,
    event.regular_event_name,
    event.loyalty_event,
    po.allocated_qty_online,
    po.received_qty_online,
    po.allocated_qty_store,
    po.received_qty_store,
    prod.web_style_num,
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
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
  from (
    select * from {ip_forecast_t2_schema}.idf_online_inv
    where selling_channel = 'ONLINE') inv
  join {ip_forecast_t2_schema}.idf_product_view prod
    on inv.epm_choice_num = prod.epm_choice_num
  left join (select 
    channel_brand,
    selling_channel,
    epm_choice_num,
    week_start_date,
    sum(order_line_quantity) order_line_quantity,
    sum(bopus_order_line_quantity) as bopus_order_line_quantity,
    sum(dropship_order_line_quantity) as dropship_order_line_quantity
    from {ip_forecast_t2_schema}.idf_online_sale
    group by channel_brand,selling_channel,epm_choice_num,week_start_date) sales
    on inv.epm_choice_num = sales.epm_choice_num
    and inv.week_start_date = sales.week_start_date
    and inv.selling_channel = sales.selling_channel
    and inv.channel_brand = sales.channel_brand
  left join (select 
    channel_brand,
    selling_channel,
    epm_choice_num,
    week_start_date,
    sum(order_line_quantity) order_line_quantity,
    sum(bopus_order_line_quantity) as bopus_order_line_quantity,
    sum(dropship_order_line_quantity) as dropship_order_line_quantity
    from {ip_forecast_t2_schema}.idf_online_sale_hist
    group by channel_brand,selling_channel,epm_choice_num,week_start_date) sales_hist
    on inv.epm_choice_num = sales_hist.epm_choice_num
    and inv.week_start_date = sales_hist.week_start_date
    and inv.selling_channel = sales_hist.selling_channel
    and inv.channel_brand = sales_hist.channel_brand
  left join (select 
    channel_brand,
    selling_channel, 
    epm_choice_num,
    week_start_date,
    sum(gross_sls_ttl_u) merch_gross_sales,
    sum(gross_sls_ttl_reg_u) merch_regular_gross_sales
    from {ip_forecast_t2_schema}.idf_online_sale_merch 
    group by channel_brand,selling_channel,epm_choice_num,week_start_date) sm
    on inv.epm_choice_num = sm.epm_choice_num
    and inv.week_start_date = sm.week_start_date
    and inv.selling_channel = sm.selling_channel
    and inv.channel_brand = sm.channel_brand
  left join {ip_forecast_t2_schema}.idf_dropship drop_inv
    on inv.epm_choice_num = drop_inv.epm_choice_num
    and inv.week_start_date = drop_inv.week_start_date
    and inv.channel_brand = drop_inv.channel_brand
  left join {ip_forecast_t2_schema}.idf_digital digital
    on inv.epm_choice_num = digital.epm_choice_num
    and inv.week_start_date = digital.week_start_date
    and inv.selling_channel = digital.selling_channel
    and inv.channel_brand = digital.channel_brand
  left join {ip_forecast_t2_schema}.idf_price price
    on inv.epm_choice_num = price.epm_choice_num
    and inv.week_start_date= price.week_start_date
    and inv.selling_channel = price.selling_channel
    and inv.channel_brand = price.channel_brand
  left join {ip_forecast_t2_schema}.idf_price_event price_event
		on inv.epm_choice_num = price_event.epm_choice_num
		and inv.week_start_date = price_event.week_start_date
		and inv.selling_channel = price_event.selling_channel
		and inv.channel_brand = price_event.channel_brand
  left join {ip_forecast_t2_schema}.idf_rp rp
    on inv.epm_choice_num = rp.epm_choice_num
    and inv.week_start_date = rp.week_start_date
    and inv.selling_channel = rp.selling_channel
    and inv.channel_brand = rp.channel_brand
  left join {ip_forecast_t2_schema}.idf_events event
    on inv.week_start_date = event.week_start_date
    and inv.selling_channel = event.selling_channel
    and inv.channel_brand = event.channel_brand
  left join {ip_forecast_t2_schema}.idf_po po
    on inv.epm_choice_num = po.epm_choice_num
    and inv.week_start_date = po.week_start_date
    and inv.selling_channel = po.selling_channel
    and inv.channel_brand = po.channel_brand;


/*
T2/View Name: IDF_MASTER_STORE_VW
Team/Owner: Item Demand Forecasting Team (SCDS)
Date Created/Modified: 2023-11

Note:
This table pulls together all of the idf feature tables into one master using store inventory and store sales as a base 
The product view is merged into this table to get the most up to date product information
This view only contains the store demand by CC x Week

*/
REPLACE VIEW {ip_forecast_t2_schema}.IDF_MASTER_STORE_VW AS LOCK ROW FOR ACCESS
  SELECT distinct
    inv.week_num,
    inv.week_start_date,
    inv.selling_channel,
    prod.dept_num,
    inv.channel_brand,
    inv.epm_choice_num,
    inv.regular_price_amt,
    inv.boh,
    inv.boh_sku_ct,
    inv.boh as boh_store,
    inv.boh_sku_ct as boh_store_sku_ct,
    inv.boh_store_ct,
    sales.demand_quantity demand,
    sales.store_take_demand as demand_store_take,
    sales.owned_demand as demand_owned,
    sales.demand_quantity-sales.owned_demand as demand_dropship,
    sales_hist.demand_quantity demand_hist,
    sales_hist.store_take_demand as demand_store_take_hist,
    sales_hist.owned_demand as demand_owned_hist,
    sales_hist.demand_quantity-sales_hist.owned_demand as demand_dropship_hist,
    sm.merch_gross_sales,
    sm.merch_regular_gross_sales,
    drop_inv.dropship_boh,
    drop_inv.dropship_sku_ct,
    digital.averagerating,
    digital.product_views,
    digital.add_to_bag,
    price.current_price_amt,
    price.current_price_type,
    price_event.current_price_event,
	  price_event.event_tags,
    rp.rp_ind,
    event.regular_event_name,
    event.loyalty_event,
    po.allocated_qty_online, 
    po.received_qty_online,
    po.allocated_qty_store,
    po.received_qty_store,
    prod.web_style_num,
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
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
  from (
    select channel_brand,
    selling_channel,
    epm_choice_num,
    week_num,
    week_start_date,
    max(regular_price_amt) regular_price_amt,
    sum(boh) as boh, 
    max(boh_sku_ct) as boh_sku_ct, 
    count(distinct store_num) as boh_store_ct
    from {ip_forecast_t2_schema}.idf_store_inv
    group by channel_brand,selling_channel,epm_choice_num,week_num, week_start_date
    having selling_channel = 'STORE') inv
  join {ip_forecast_t2_schema}.idf_product_view prod
    on inv.epm_choice_num = prod.epm_choice_num
  left join
  (select channel_brand,
    selling_channel,
    epm_choice_num,
    week_start_date,
    sum(demand_quantity) demand_quantity,
    sum(store_take_demand) as store_take_demand, 
    sum(owned_demand) as owned_demand
    from {ip_forecast_t2_schema}.idf_store_sale
    group by channel_brand,selling_channel,epm_choice_num,week_start_date) sales
    on inv.epm_choice_num = sales.epm_choice_num
    and inv.week_start_date = sales.week_start_date
    and inv.selling_channel = sales.selling_channel
    and inv.channel_brand = sales.channel_brand
  left join
  (select channel_brand,
    selling_channel,
    epm_choice_num,
    week_start_date,
    sum(demand_quantity) demand_quantity,
    sum(store_take_demand) as store_take_demand, 
    sum(owned_demand) as owned_demand
    from {ip_forecast_t2_schema}.idf_store_sale_hist
    group by channel_brand,selling_channel,epm_choice_num,week_start_date) sales_hist
    on inv.epm_choice_num = sales_hist.epm_choice_num
    and inv.week_start_date = sales_hist.week_start_date
    and inv.selling_channel = sales_hist.selling_channel
    and inv.channel_brand = sales_hist.channel_brand
  left join (select 
    channel_brand,
    selling_channel,
    epm_choice_num,
    week_start_date,
    sum(gross_sls_ttl_u) merch_gross_sales,
    sum(gross_sls_ttl_reg_u) merch_regular_gross_sales
    from {ip_forecast_t2_schema}.idf_store_sale_merch 
    group by channel_brand,selling_channel,epm_choice_num,week_start_date) sm
    on inv.epm_choice_num = sm.epm_choice_num
    and inv.week_start_date = sm.week_start_date
    and inv.selling_channel = sm.selling_channel
    and inv.channel_brand = sm.channel_brand
  left join {ip_forecast_t2_schema}.idf_dropship drop_inv
    on inv.epm_choice_num = drop_inv.epm_choice_num
    and inv.week_start_date = drop_inv.week_start_date
    and inv.channel_brand = drop_inv.channel_brand
  left join {ip_forecast_t2_schema}.idf_digital digital
    on inv.epm_choice_num = digital.epm_choice_num
    and inv.week_start_date = digital.week_start_date
    and inv.channel_brand = digital.channel_brand
  left join {ip_forecast_t2_schema}.idf_price price
    on inv.epm_choice_num = price.epm_choice_num
    and inv.week_start_date = price.week_start_date
    and inv.selling_channel = price.selling_channel
    and inv.channel_brand = price.channel_brand
  left join {ip_forecast_t2_schema}.idf_price_event price_event
		on inv.epm_choice_num = price_event.epm_choice_num
		and inv.week_start_date = price_event.week_start_date
		and inv.selling_channel = price_event.selling_channel
		and inv.channel_brand = price_event.channel_brand
  left join {ip_forecast_t2_schema}.idf_rp rp
    on inv.epm_choice_num = rp.epm_choice_num
    and inv.week_start_date= rp.week_start_date
    and inv.selling_channel = rp.selling_channel
    and inv.channel_brand = rp.channel_brand
  left join {ip_forecast_t2_schema}.idf_events event
    on inv.week_start_date = event.week_start_date
    and inv.selling_channel = event.selling_channel
    and inv.channel_brand = event.channel_brand
  left join {ip_forecast_t2_schema}.idf_po po
    on inv.epm_choice_num = po.epm_choice_num
    and inv.week_start_date= po.week_start_date
    and inv.selling_channel = po.selling_channel
    and inv.channel_brand = po.channel_brand;

/*
T2/View Name: IDF_MASTER_VW
Team/Owner: Item Demand Forecasting Team (SCDS)
Date Created/Modified: 2023-11

Note:
This view pulls together the online and store master viewss into one unified view
This view will then be exported to S3

*/
REPLACE VIEW {ip_forecast_t2_schema}.IDF_MASTER_VW AS LOCK ROW FOR ACCESS
  SELECT * from {ip_forecast_t2_schema}.IDF_MASTER_STORE_VW
  union all
  SELECT * from {ip_forecast_t2_schema}.IDF_MASTER_ONLINE_VW;



/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;