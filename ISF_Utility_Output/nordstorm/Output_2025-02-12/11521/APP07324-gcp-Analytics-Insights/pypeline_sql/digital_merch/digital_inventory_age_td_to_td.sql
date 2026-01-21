
/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP09142;
     DAG_ID=digital_inventory_age_table_11521_ACE_ENG;
     Task_Name=digital_inventory_age_td_to_td;'
     FOR SESSION VOLATILE;


/*
T2/Table Name: t2dl_das_digitalmerch_flash
Team/Owner: Nicole Miao, Sean Larkin, Rae Ann Boswell
Date Created/Modified: 08/26/2024
*/



--- Step 0 Date Lookup
create multiset volatile table date_lookup as (
    select max(week_num) as max_week
    from prd_nap_usr_vws.day_cal 
    where day_date = current_date()
) WITH DATA ON COMMIT PRESERVE ROWS;


--- Step 1 SKU Lookup Table
CREATE VOLATILE MULTISET TABLE sku_lkp AS (
    SELECT
    	rms_sku_num AS sku_idnt
        , channel_country AS country
        , CASE WHEN channel_country = 'US' THEN 'USA'
			   WHEN channel_country = 'CA' THEN 'CAN'
		  ELSE '' END AS price_country
        , rms_style_num
        , color_num
        , CASE WHEN COALESCE(color_num,'0') IN ('0', '00', '000') THEN '0' ELSE LTRIM(color_num, '0') END AS colr_idnt
        , cc AS cc_idnt
		, CASE WHEN (color_desc LIKE '%Beige%' OR color_desc LIKE '%Black%' OR color_desc LIKE '%Grey%' OR color_desc LIKE '%White%') THEN 'Core'
            ELSE 'Other' END AS colr_flag
        , style_group_num
        , web_style_num
        , vpn
        , prmy_supp_num
        , sbclass_num
        , sbclass_desc
        , class_num
        , class_desc
        , dept_num
        , dept_desc
        , grp_num
        , grp_desc
        , div_num
        , div_desc
        , brand_name
        , type_level_1_num
        , type_level_2_num
        , size_1_num
        , size_2_num
        , dropship_ind
        , dw_sys_load_tmstp
        , selling_status_code
        , age_groups_desc
        , genders_desc
        , live_date
        , npg_flag AS npg_ind
        , CASE WHEN (div_num = '351' AND grp_num = '770' AND dept_num = '874' AND class_num IN ('93', '94', '96')) THEN 'samples'
              WHEN (div_num = '340' AND grp_num = '740' AND dept_num IN ('813', '814', '815', '816', '817', '818') AND class_num = '90' ) THEN 'samples'
              WHEN (div_num = '340' AND grp_num = '750' AND dept_num = '812' AND class_num = '90' ) THEN 'samples'
              WHEN (div_num = '340' AND dept_num IN ('584', '585', '523')) THEN 'samples'
              WHEN (div_num = '700' AND grp_num = '7200' AND dept_num = '587' AND class_num = '55') THEN 'samples'
          ELSE 'not_samples' END AS sample_idnt
    FROM T2DL_DAS_MARKDOWN.SKU_LKP
    WHERE NOT dept_num IN ('584','585','523') AND NOT (div_num = '340' AND class_num = '90')
    	AND sample_idnt = 'not_samples'
    	AND cc IS NOT NULL
		AND grp_desc IS NOT NULL
) WITH DATA PRIMARY INDEX(sku_idnt) ON COMMIT PRESERVE ROWS;




--- Step 2 ncom SKU List
CREATE MULTISET VOLATILE TABLE ncom_sku AS (
 select distinct bs.sku_idnt
        ,psd.div_desc
        ,b.grp_desc
        ,psd.brand_name
        ,bs.day_date
        ,b.cc_idnt
        ,bs.customer_choice
        ,bs.days_published
        ,bs.days_live
        ,bs.new_flag
        ,current_price_type
        ,rp_flag
        ,dropship_flag
        ,sales_dollars
        ,demand_dollars
        ,bs.product_views
        ,bs.demand_units
        ,net_sales_reg_units
        ,net_sales_clr_units
        ,net_sales_pro_units
        ,net_sales_reg_dollars
        ,net_sales_clr_dollars
        ,net_sales_pro_dollars
    from t2dl_das_selection.selection_productivity_base as bs
    join sku_lkp as b on trim(bs.sku_idnt) = trim(b.sku_idnt)
    join PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW AS psd ON psd.rms_sku_num = bs.sku_idnt
    join t2dl_das_product_funnel.product_price_funnel_daily as pt on pt.rms_sku_num = trim(bs.sku_idnt) and pt.event_date_pacific = bs.day_date
    where bs.channel_country in ('US')
    and channel_brand in ('NORDSTROM') ---- Change this one to the right banner
    and bs.channel in ('DIGITAL') and days_published is not null
    and psd.channel_country IN ('US')
    )with data primary index(sku_idnt,div_desc,day_date) on commit preserve rows;

-- Rcom SKU List
CREATE MULTISET VOLATILE TABLE rcom_sku AS (
 select distinct bs.sku_idnt
        ,psd.div_desc
        ,b.grp_desc
        ,psd.brand_name
        ,bs.day_date
        ,b.cc_idnt
        ,bs.customer_choice
        ,bs.days_published
        ,bs.days_live
        ,bs.new_flag
        ,current_price_type
        ,rp_flag
        ,dropship_flag
        ,sales_dollars
        ,demand_dollars
        ,bs.product_views
        ,bs.demand_units
        ,net_sales_reg_units
        ,net_sales_clr_units
        ,net_sales_pro_units
        ,net_sales_reg_dollars
        ,net_sales_clr_dollars
        ,net_sales_pro_dollars
    from t2dl_das_selection.selection_productivity_base as bs
    join sku_lkp as b on trim(bs.sku_idnt) = trim(b.sku_idnt)
    join PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW AS psd ON psd.rms_sku_num = bs.sku_idnt
    join t2dl_das_product_funnel.product_price_funnel_daily as pt on pt.rms_sku_num = trim(bs.sku_idnt) and pt.event_date_pacific = bs.day_date
    where bs.channel_country in ('US')
    and channel_brand in ('NORDSTROM_RACK') ---- Change this one to the right banner
    and bs.channel in ('DIGITAL') and days_published is not null
    and psd.channel_country IN ('US')
    )with data primary index(sku_idnt,div_desc,day_date) on commit preserve rows;





--- Step 3 Summarizing
CREATE MULTISET VOLATILE TABLE ncom_cc AS (
 select distinct sku_idnt
        ,div_desc
        ,grp_desc
        ,brand_name
        ,week_num
        ,current_price_type
        ,sum(new_flag) as new_flag
        ,sum(rp_flag) as rp_flag
        ,sum(sales_dollars) as sales_dollars
        ,sum(demand_dollars) as demand_dollars
        ,sum(product_views) as product_views
        ,sum(demand_units) as order_quantity
        ,sum(net_sales_reg_units)  as net_sales_reg_units
        ,sum(net_sales_clr_units) as net_sales_clr_units
        ,sum(net_sales_pro_units) as net_sales_pro_units
        ,sum(net_sales_reg_dollars) as net_sales_reg_dollars
        ,sum(net_sales_clr_dollars) as net_sales_clr_dollars
        ,sum(net_sales_pro_dollars) as net_sales_pro_dollars
    from ncom_sku as a
    join PRD_NAP_USR_VWS.day_cal as b on a.day_date = b.day_date
    group by 1,2,3,4,5,6
    where div_desc in ('HOME','APPAREL','SHOES','ACCESSORIES','BEAUTY','DESIGNER')
)with data primary index(sku_idnt) on commit preserve rows;

-- Rcom_CC
CREATE MULTISET VOLATILE TABLE rcom_cc AS (
 select distinct sku_idnt
        ,div_desc
        ,grp_desc
        ,brand_name
        ,week_num
        ,current_price_type
        ,sum(new_flag) as new_flag
        ,sum(rp_flag) as rp_flag
        ,sum(sales_dollars) as sales_dollars
        ,sum(demand_dollars) as demand_dollars
        ,sum(product_views) as product_views
        ,sum(demand_units) as order_quantity
        ,sum(net_sales_reg_units)  as net_sales_reg_units
        ,sum(net_sales_clr_units) as net_sales_clr_units
        ,sum(net_sales_pro_units) as net_sales_pro_units
        ,sum(net_sales_reg_dollars) as net_sales_reg_dollars
        ,sum(net_sales_clr_dollars) as net_sales_clr_dollars
        ,sum(net_sales_pro_dollars) as net_sales_pro_dollars
    from rcom_sku as a
    join PRD_NAP_USR_VWS.day_cal as b on a.day_date = b.day_date
    group by 1,2,3,4,5,6
    where div_desc in ('HOME','APPAREL','SHOES','ACCESSORIES','BEAUTY','DESIGNER')
)with data primary index(sku_idnt) on commit preserve rows;



--- Step 4 
CREATE MULTISET VOLATILE TABLE ncom_merch_groups_sku AS (
 select distinct sku_idnt
        ,category
        ,a.div_desc
        ,a.grp_desc
        ,a.brand_name
        ,a.week_num
        ,current_price_type
        ,ROW_NUMBER() OVER (PARTITION BY a.sku_idnt ORDER BY a.week_num) as weeks_count
        ,cast(RIGHT(trim(a.week_num),2) as int) as weeks_count2
        ,cast(left(trim(a.week_num),4) as int) as year_num
        ,min(day_date) as week_min_date
        ,max(day_date) as week_max_date
        ,sum(new_flag) as new_flag
        ,sum(rp_flag) as rp_flag
        ,sum(sales_dollars) as sales_dollars
        ,sum(demand_dollars) as demand_dollars
        ,sum(product_views) as product_views
        ,sum(a.order_quantity) as order_quantity
        ,sum(net_sales_reg_units)  as net_sales_reg_units
        ,sum(net_sales_clr_units) as net_sales_clr_units
        ,sum(net_sales_pro_units) as net_sales_pro_units
        ,sum(net_sales_reg_dollars) as net_sales_reg_dollars
        ,sum(net_sales_clr_dollars) as net_sales_clr_dollars
        ,sum(net_sales_pro_dollars) as net_sales_pro_dollars
    from ncom_cc as a
    join T2DL_DAS_MARKDOWN.sku_LKP as b on trim(a.sku_idnt) = trim(b.rms_sku_num) and channel_country = 'US'
    left join prd_nap_usr_vws.day_cal dc on dc.week_num = a.week_num
    group by 1,2,3,4,5,6,7
    where a.div_desc in ('HOME','APPAREL','SHOES','ACCESSORIES','BEAUTY','DESIGNER')
    and b.CHANNEL_COUNTRY in ('US')
)with data primary index(sku_idnt) on commit preserve rows;

CREATE MULTISET VOLATILE TABLE rcom_merch_groups_sku AS (
 select distinct sku_idnt
        ,category
        ,a.div_desc
        ,a.grp_desc
        ,a.brand_name
        ,a.week_num
        ,current_price_type
        ,ROW_NUMBER() OVER (PARTITION BY a.sku_idnt ORDER BY a.week_num) as weeks_count
        ,cast(RIGHT(trim(a.week_num),2) as int) as weeks_count2
        ,cast(left(trim(a.week_num),4) as int) as year_num
        ,min(day_date) as week_min_date
        ,max(day_date) as week_max_date
        ,sum(new_flag) as new_flag
        ,sum(rp_flag) as rp_flag
        ,sum(sales_dollars) as sales_dollars
        ,sum(demand_dollars) as demand_dollars
        ,sum(product_views) as product_views
        ,sum(a.order_quantity) as order_quantity
        ,sum(net_sales_reg_units)  as net_sales_reg_units
        ,sum(net_sales_clr_units) as net_sales_clr_units
        ,sum(net_sales_pro_units) as net_sales_pro_units
        ,sum(net_sales_reg_dollars) as net_sales_reg_dollars
        ,sum(net_sales_clr_dollars) as net_sales_clr_dollars
        ,sum(net_sales_pro_dollars) as net_sales_pro_dollars
    from rcom_cc as a
    join T2DL_DAS_MARKDOWN.sku_LKP as b on trim(a.sku_idnt) = trim(b.rms_sku_num) and channel_country = 'US'
    left join prd_nap_usr_vws.day_cal dc on dc.week_num = a.week_num
    group by 1,2,3,4,5,6,7
    where a.div_desc in ('HOME','APPAREL','SHOES','ACCESSORIES','BEAUTY','DESIGNER')
    and b.CHANNEL_COUNTRY in ('US')
)with data primary index(sku_idnt) ON COMMIT PRESERVE ROWS;



--- Step 5
CREATE MULTISET volatile TABLE ncom_user_cohorts AS(
SELECT  sku_idnt
        ,category
        ,div_desc
        ,grp_desc
        ,coalesce(grp_desc||'-', '')||coalesce(category||'', '') as subdiv_cat
       ,min(week_num) as cohortweek
       ,min(week_min_date) as cohortweek_min_date
    FROM  ncom_merch_groups_sku
    GROUP BY 1,2,3,4,5
)with data primary index(sku_idnt) on commit preserve rows;

CREATE MULTISET volatile TABLE rcom_user_cohorts AS(
SELECT  sku_idnt
        ,category
        ,div_desc
        ,grp_desc
        ,coalesce(grp_desc||'-', '')||coalesce(category||'', '') as subdiv_cat
       ,min(week_num) as cohortweek
       ,min(week_min_date) as cohortweek_min_date
    FROM  rcom_merch_groups_sku
    GROUP BY 1,2,3,4,5
)with data primary index(sku_idnt) on commit preserve rows;






--- Step 6
create multiset volatile table tempmmm_ncom as (
    SELECT distinct sku_idnt, min(week_min_date) as start_week, max(week_min_date) as end_week from  ncom_merch_groups_sku
    having max(week_num) <= (select max_week from date_lookup) and min(week_num) > 202103 group by 1
)with data primary index(sku_idnt) on commit preserve rows;

create multiset volatile table tempmmm_rcom as (
SELECT distinct sku_idnt, min(week_min_date) as start_week, max(week_min_date) as end_week from  rcom_merch_groups_sku
having max(week_num) <= (select max_week from date_lookup) and min(week_num) > 202103 group by 1
)with data primary index(sku_idnt) on commit preserve rows;




--- Step 7
CREATE MULTISET VOLATILE TABLE ncom_merch_groups_agg AS (
    select a.sku_idnt
          ,a.div_desc
          ,a.grp_desc
          ,current_price_type
          ,subdiv_cat
          ,a.week_num
          ,cohortweek
          ,(a.week_max_date - cohortweek_min_date)/7 +1 as inventory_age
          ,count(distinct a.sku_idnt) as cc_count
          ,sum(new_flag) as new_flag_count
          ,sum(rp_flag) as rp_flag_count
          ,sum(sales_dollars) as sales_dollars
          ,sum(demand_dollars) as demand_dollars
          ,sum(order_quantity) as order_quantity
          ,sum(a.product_views) as product_views
          ,sum(net_sales_reg_units) as net_sales_reg_units
    from ncom_merch_groups_sku as a
    LEFT JOIN ncom_user_cohorts as b on a.sku_idnt = b.sku_idnt
    inner join tempmmm_ncom as c on a.sku_idnt = c.sku_idnt
    group by 1,2,3,4,5,6,7,8
    where a.div_desc in ('HOME','APPAREL','SHOES','ACCESSORIES','BEAUTY','DESIGNER') and current_price_type in ('R')
)with data primary index(sku_idnt,week_num) on commit preserve rows;

CREATE MULTISET VOLATILE TABLE rcom_merch_groups_agg AS (
    select a.sku_idnt
          ,a.div_desc
          ,a.grp_desc
          ,current_price_type
          ,subdiv_cat
          ,a.week_num
          ,cohortweek
          ,(a.week_max_date - cohortweek_min_date)/7 +1 as inventory_age
          ,count(distinct a.sku_idnt) as cc_count
          ,sum(new_flag) as new_flag_count
          ,sum(rp_flag) as rp_flag_count
          ,sum(sales_dollars) as sales_dollars
          ,sum(demand_dollars) as demand_dollars
          ,sum(order_quantity) as order_quantity
          ,sum(a.product_views) as product_views
          ,sum(net_sales_reg_units) as net_sales_reg_units
    from rcom_merch_groups_sku as a
    LEFT JOIN rcom_user_cohorts as b on a.sku_idnt = b.sku_idnt
    inner join tempmmm_rcom as c on a.sku_idnt = c.sku_idnt
    group by 1,2,3,4,5,6,7,8
    where a.div_desc in ('HOME','APPAREL','SHOES','ACCESSORIES','BEAUTY','DESIGNER') and current_price_type in ('R')
)with data primary index(sku_idnt, week_num) on commit preserve rows;



create multiset volatile table xcom_inventory_age as (
    select cast('NORDSTROM' as varchar(100)) as channel_brand,
        ncom.* 
    from ncom_merch_groups_agg ncom
    union all
    select cast('NORDSTROM_RACK' as varchar(100)) as channel_brand,
            rcom.* 
    from rcom_merch_groups_agg rcom
)with data primary index(sku_idnt,week_num) on commit preserve rows;


DELETE 
FROM    {digital_merch_t2_schema}.digital_inventory_age
;

INSERT INTO {digital_merch_t2_schema}.digital_inventory_age
select a.*,
    CURRENT_TIMESTAMP as dw_sys_load_tmstp 
from xcom_inventory_age a
;


COLLECT STATS COLUMN (PARTITION),
            COLUMN (sku_idnt,week_num), -- column names used for primary index
            COLUMN (week_num)  -- column names used for partition
on {digital_merch_t2_schema}.digital_inventory_age;



/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;


