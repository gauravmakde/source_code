/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08073;
     DAG_ID=item_demand_forecasting_product_view_11521_ACE_ENG;
     Task_Name=create_product_view;'
     FOR SESSION VOLATILE;

/*

T2/View Name: IDF_PRODUCT_VW
Team/Owner: Item Demand Forecasting Team (SCDS)
Date Created/Modified: 2023-11

Note:
Product table contains the most up to date product hierarchy (Dept/Class/Supplier/Brand)  information for every item at nordstrom
This is merged to feature tables every week to ensure the product hierarchy information is always up to date (captures any re-classes)

*/ 

REPLACE VIEW {ip_forecast_t2_schema}.IDF_PRODUCT_VIEW AS LOCK ROW FOR ACCESS
  select
    distinct cast(prod.epm_choice_num as bigint) as epm_choice_num,
    max(prod.class_num) as class_num,
    max(prod.sbclass_num) as sbclass_num,
    upper(min(prod.brand_name)) as brand_name,
    min(prod.prmy_supp_num) as prmy_supp_num,
    min(prod.manufacturer_num) as manufacturer_num,
    min(prod.color_num) as color_num,
    min(clr.color_desc) as color_desc,
    min(prod.epm_style_num) as epm_style_num,
    max(prod.web_style_num) as web_style_num,
    min(prod.style_desc) as style_desc,
    min(
        case
            when prod.live_date > current_date then null
            else prod.live_date
        end 
    ) as live_date, 
    current_timestamp(0) at time zone 'gmt' as last_updated_utc,
    prod.dept_num,
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
from 
    PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW as prod
    left join PRD_NAP_USR_VWS.PRODUCT_COLOR_DIM_VW as clr on prod.color_num = clr.color_num
where
    prod.channel_country = 'US'
    and prod.epm_choice_num is not null
		and dept_desc not like 'INACT%'
    and div_desc not like 'INACT%'
group by 
    prod.epm_choice_num,
    prod.dept_num;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION; 