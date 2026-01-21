SET QUERY_BAND = 'App_ID=APP08227;
     DAG_ID=product_price_funnel_daily_11521_ACE_ENG;
     Task_Name=divisional_style_daily;'
     FOR SESSION VOLATILE;


-- FILENAME: divisional_style_daily.sql
-- GOAL: Roll up in-stock rates for summary dashboard in tableau
-- AUTHOR: Meghan Hickey (meghan.d.hickey@nordstrom.com)

-- full reload to restate product hierarchy
DELETE from T2DL_DAS_PRODUCT_FUNNEL.divisional_style_daily;

-- 1. Pull Sku into Table
create multiset volatile table sku_base as (
    SELECT
        sku.rms_sku_num
        , sku.channel_country
        , sku.web_style_num
        , sty.style_group_num
        , CAST(sku.div_num AS VARCHAR(20)) || ', ' || sku.div_desc AS division
        , CAST(sku.grp_num AS VARCHAR(20)) || ', ' || sku.grp_desc AS subdiv
        , CAST(sku.dept_num AS VARCHAR(20)) || ', ' || sku.dept_desc AS department
        , CAST(sku.class_num AS VARCHAR(20)) || ', ' || sku.class_desc AS "class"
        , CAST(sku.sbclass_num AS VARCHAR(20)) || ', ' || sku.sbclass_desc AS subclass
        , UPPER(sku.brand_name) AS brand_name
        , UPPER(supp.vendor_name) as supplier
        , UPPER(sty.type_level_1_desc) AS product_type_1
        , UPPER(sty.style_desc) AS style_desc
        , sku.partner_relationship_type_code -- MP
    FROM
        prd_nap_usr_vws.product_sku_dim_vw sku
        INNER JOIN prd_nap_usr_vws.product_style_dim sty
            ON sku.epm_style_num = sty.epm_style_num
            AND sku.channel_country = sty.channel_country
        LEFT JOIN prd_nap_usr_vws.vendor_dim supp
            ON sku.prmy_supp_num =supp.vendor_num
        --Exclude GWP
        WHERE NOT sku.dept_num IN ('584', '585', '523') AND NOT (sku.div_num = '340' AND sku.class_num = '90')
) with data primary index(rms_sku_num, channel_country) on commit preserve rows;

collect stats primary index(rms_sku_num, channel_country), column(rms_sku_num, channel_country) on sku_base;

-- 2. Create Date lookup
create multiset volatile table rolling_ty as (
    select
        day_date as day_dt
        , week_end_day_date as week_end_date
        , day_date_last_year_realigned
        , fiscal_year_num as fiscal_year
        , fiscal_quarter_num
        , quarter_desc as fiscal_quarter
        , fiscal_month_num
        , month_desc as fiscal_month
        , fiscal_week_num as fiscal_week
        , week_desc
        , fiscal_day_num as fiscal_day
    from prd_nap_usr_vws.day_cal_454_dim
    where week_end_day_date <= (SELECT MAX(event_date_pacific) FROM t2dl_das_product_funnel.product_funnel_daily) + 7
    QUALIFY DENSE_RANK() OVER (ORDER BY week_end_day_date DESC) <= 5
) with data primary index (day_dt) on commit preserve rows
;

create multiset volatile table ty_ly as (
    select
        day_dt
        , week_end_date
        , fiscal_day
        , fiscal_week
        , fiscal_month
        , fiscal_year
        , fiscal_quarter
        , 'TY' as ty_ly_ind
    from rolling_ty
    union all
    select
        b.day_date as day_dt
        , b.week_end_day_date as week_end_date
        , a.fiscal_day
        , a.fiscal_week
        , a.fiscal_month
        , a.fiscal_year - 1 as fiscal_year
        , a.fiscal_quarter
        , 'LY' as ty_ly_ind
    from rolling_ty a
    left join prd_nap_usr_vws.day_cal_454_dim b
    on a.day_date_last_year_realigned = b.day_date
) with data primary index (day_dt) on commit preserve rows
;


-- Temp Step to Avoid LOS Dupes
create multiset volatile table pfd as (
    select
          pf. event_date_pacific
          ,pf.rms_sku_num
          , pf.channelcountry
          , pf.channel
          , pf.platform
          , pf.web_style_num AS web_style_id
          , pf.regular_price_amt
          , pf.current_price_amt
          , pf.current_price_type
          , pf.rp_ind
          , pf.event_type
          , pf.event_name
          , sum(pf.product_views) AS product_views
          , sum(pf.product_view_sessions) AS product_view_sessions
          , sum(pf.add_to_bag_quantity) AS add_to_bag_quantity
          , sum(pf.add_to_bag_sessions) AS add_to_bag_sessions
          , sum(pf.order_demand) AS order_demand
          , sum(pf.order_quantity) AS order_quantity
          , sum(pf.order_sessions) AS order_sessions
          , sum(pf.product_views*pf.pct_instock) AS instock_views
          , sum(case when pf.pct_instock is not null then pf.product_views end) AS scored_views
    from T2DL_DAS_PRODUCT_FUNNEL.product_price_funnel_daily pf
    where event_date_pacific >= (select min(day_dt) from ty_ly)
    group by 1,2,3,4,5,6,7,8,9,10,11,12
) with data no primary index on commit preserve rows;

collect stats column(rms_sku_num, channelcountry), column(event_date_pacific) on pfd;


-- 3. Filter to relevant dates
create multiset volatile table filter_dates as (
    SELECT
        pf.*
        , ty_ly.day_dt as activity_date
        , ty_ly.week_end_date
        , ty_ly.fiscal_day
        , ty_ly.fiscal_week
        , ty_ly.fiscal_month
        , ty_ly.fiscal_year
        , ty_ly.fiscal_quarter
        , ty_ly.ty_ly_ind
    FROM pfd pf
    INNER JOIN ty_ly
        ON pf.event_date_pacific = ty_ly.day_dt
) with data primary index(rms_sku_num, channelcountry) on commit preserve rows;

collect stats primary index(rms_sku_num, channelcountry), column(rms_sku_num, channelcountry) on filter_dates;

-- 4. Join in product hierarchy
create multiset volatile table pfd_sku as (
    SELECT
        pf.event_date_pacific as activity_date
        , pf.week_end_date
        , pf.fiscal_day
        , pf.fiscal_week
        , pf.fiscal_month
        , pf.fiscal_year
        , pf.fiscal_quarter
        , pf.ty_ly_ind
        , pf.channelcountry
        , pf.channel
        , pf.platform
        , COALESCE(sku_base.division, 'UNKNOWN') AS division
        , COALESCE(sku_base.subdiv, 'UNKNOWN') AS subdiv
        , COALESCE(sku_base.department, 'UNKNOWN') AS department
        , COALESCE(sku_base."class", 'UNKNOWN') AS "class"
        , COALESCE(sku_base.subclass, 'UNKNOWN') AS subclass
        , COALESCE(sku_base.brand_name, 'UNKNOWN') AS brand_name
        , COALESCE(sku_base.supplier, 'UNKNOWN') AS supplier
        , COALESCE(sku_base.product_type_1, 'UNKNOWN') AS product_type_1
        , pf.web_style_id
        , COALESCE(sku_base.style_desc, 'UNKNOWN') AS style_desc
        , COALESCE(sku_base.style_group_num,'UNKNOWN') AS style_group_num
        , pf.event_type
        , pf.event_name
        , CASE WHEN rp_ind = 1 then 'RP' else 'NRP' end as rp
        , CASE WHEN partner_relationship_type_code = 'ECONCESSION' THEN 'Y' ELSE 'N' END AS mp_ind 
        , coalesce(current_price_type, 'UNKNOWN') as price_type
        , CASE
            WHEN current_price_amt = 0 OR current_price_amt IS NULL THEN 'UNKNOWN'
            WHEN current_price_amt <= 10.00 THEN '< $10'
            WHEN current_price_amt <= 25.00 THEN '$10 - $25'
            WHEN current_price_amt <= 50.00 THEN '$25 - $50'
            WHEN current_price_amt <= 100.00 THEN '$50 - $100'
            WHEN current_price_amt <= 150.00 THEN '$100 - $150'
            WHEN current_price_amt <= 200.00 THEN '$150 - $200'
            WHEN current_price_amt <= 300.00 THEN '$200 - $300'
            WHEN current_price_amt <= 500.00 THEN '$300 - $500'
            WHEN current_price_amt <= 1000.00 THEN '$500 - $1000'
            WHEN current_price_amt >  1000.00 THEN '> $1000'
            END AS price_band_one
        , CASE
            WHEN current_price_amt = 0 OR current_price_amt IS NULL THEN 'UNKNOWN'
            WHEN current_price_amt <= 10.00 THEN '< $10'
            WHEN current_price_amt <= 15.00 THEN '$10 - $15'
            WHEN current_price_amt <= 20.00 THEN '$15 - $20'
            WHEN current_price_amt <= 25.00 THEN '$20 - $25'
            WHEN current_price_amt <= 30.00 THEN '$25 - $30'
            WHEN current_price_amt <= 40.00 THEN '$30 - $40'
            WHEN current_price_amt <= 50.00 THEN '$40 - $50'
            WHEN current_price_amt <= 60.00 THEN '$50 - $60'
            WHEN current_price_amt <= 80.00 THEN '$60 - $80'
            WHEN current_price_amt <= 100.00 THEN '$80 - $100'
            WHEN current_price_amt <= 125.00 THEN '$100 - $125'
            WHEN current_price_amt <= 150.00 THEN '$125 - $150'
            WHEN current_price_amt <= 175.00 THEN '$150 - $175'
            WHEN current_price_amt <= 200.00 THEN '$175 - $200'
            WHEN current_price_amt <= 250.00 THEN '$200 - $250'
            WHEN current_price_amt <= 300.00 THEN '$250 - $300'
            WHEN current_price_amt <= 400.00 THEN '$300 - $400'
            WHEN current_price_amt <= 500.00 THEN '$400 - $500'
            WHEN current_price_amt <= 700.00 THEN '$500 - $700'
            WHEN current_price_amt <= 900.00 THEN '$700 - $900'
            WHEN current_price_amt <= 1000.00 THEN '$900 - $1000'
            WHEN current_price_amt <= 1200.00 THEN '$1000 - $1200'
            WHEN current_price_amt <= 1500.00 THEN '$1200 - $1500'
            WHEN current_price_amt <= 1800.00 THEN '$1500 - $1800'
            WHEN current_price_amt <= 2000.00 THEN '$1800 - $2000'
            WHEN current_price_amt <= 3000.00 THEN '$2000 - $3000'
            WHEN current_price_amt <= 4000.00 THEN '$3000 - $4000'
            WHEN current_price_amt <= 5000.00 THEN '$4000 - $5000'
            WHEN current_price_amt >  5000.00 THEN '> $5000'
            END AS price_band_two
        , CASE WHEN regular_price_amt > 0 then 1 - (current_price_amt/regular_price_amt) else null end as discount
        , pf.product_views
        , pf.product_view_sessions
        , pf.add_to_bag_quantity
        , pf.add_to_bag_sessions
        , pf.order_demand
        , pf.order_quantity
        , pf.order_sessions
        , pf.instock_views
        , pf.scored_views
    FROM filter_dates pf
    LEFT JOIN sku_base
      ON pf.rms_sku_num = sku_base.rms_sku_num
      AND pf.channelcountry = sku_base.channel_country
) with data primary index(activity_date, web_style_id, platform) on commit preserve rows;

collect stats primary index(activity_date, web_style_id, platform), column(activity_date), column(web_style_id)
  on pfd_sku;


-- 5. Roll up from sku to style into final table
insert into T2DL_DAS_PRODUCT_FUNNEL.divisional_style_daily

    SELECT
        activity_date
        , week_end_date
        , fiscal_day
        , fiscal_week
        , fiscal_month
        , fiscal_year
        , fiscal_quarter
        , ty_ly_ind
        , channelcountry
        , channel
        , platform
        , division
        , subdiv
        , department
        , "class"
        , subclass
        , brand_name
        , supplier
        , product_type_1
        , web_style_id
        , style_desc
        , style_group_num
        , event_type
        , event_name
        , rp
        , mp_ind
        , price_type
        , price_band_one
        , price_band_two
        , discount
        , SUM(product_views) AS product_views
        , SUM(product_view_sessions) AS viewing_sessions
        , SUM(add_to_bag_quantity) AS add_to_bag_quantity
        , SUM(add_to_bag_sessions) AS adding_sessions
        , SUM(order_demand) AS order_demand
        , SUM(order_quantity) AS order_quantity
        , SUM(order_sessions) AS buying_sessions
        , SUM(instock_views) AS instock_views
        , SUM(scored_views) AS scored_views
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp
    FROM pfd_sku
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30
;

collect stats primary index(activity_date, web_style_id, platform), column(activity_date) , column(web_style_id)
  on T2DL_DAS_PRODUCT_FUNNEL.divisional_style_daily;

SET QUERY_BAND = NONE FOR SESSION;
