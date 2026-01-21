SET QUERY_BAND = 'App_ID=APP08227;
     DAG_ID=divisional_summary_weekly_11521_ACE_ENG;
     Task_Name=divisional_summary_weekly;'
     FOR SESSION VOLATILE;


-- FILENAME: divisional_summary_weekly.sql
-- GOAL: Roll up digital product funnel to weekly departmental level
-- AUTHOR: Meghan Hickey (meghan.d.hickey@nordstrom.com)

-- full reload to restate product hierarchy
DELETE from {product_funnel_t2_schema}.divisional_summary_weekly;

-- 1. Create Dates Staging Table
create multiset volatile table dts as (
    SELECT
        dt.day_dt
        , dt.wk_end_dt AS week_end_date
        --, week_idnt as week_idnt 
        , dt.wk_num_realigned AS fiscal_week
        , dt.mth_desc_realigned AS fiscal_month
        , dt.yr_idnt_realigned AS fiscal_year
        , dt.qtr_desc_realigned AS fiscal_quarter
        , ty_ly.ty_ly_ind

    FROM
        (
          SELECT
              day_date as day_dt
              , week_end_day_date as wk_end_dt
              --, week_idnt as week_idnt 
              , fiscal_year_num AS fiscal_year
              , fiscal_year_num AS yr_idnt_realigned
              , fiscal_quarter_num AS qtr_454_realigned
              , quarter_desc AS qtr_desc_realigned
              , fiscal_month_num AS mth_454_realigned
              , month_desc AS mth_desc_realigned
              , fiscal_week_num AS wk_num_realigned
              , week_desc AS wk_desc_realigned
              , fiscal_day_num AS day_num_realigned

          FROM PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW

          WHERE week_end_day_date <= (SELECT MAX(event_date_pacific) FROM t2dl_das_product_funnel.product_funnel_daily)
        ) dt
        INNER JOIN
        (
            SELECT
                fiscal_year
                , CASE yr_rank
                    WHEN 1 THEN 'TY'
                    WHEN 2 THEN 'LY'
                   -- WHEN 3 THEN 'LLY'
                    ELSE 'OTHER'
                    END AS ty_ly_ind
                , latest_wk
                --This lets us filter out prior years' weeks we haven't had this year. Check the join
                --outside this subquery to see where it happens
                , MIN(latest_wk) OVER (ORDER BY fiscal_year DESC ROWS UNBOUNDED PRECEDING) AS fiscal_week

            FROM
            (
                SELECT
                    dt.fiscal_year_num AS fiscal_year
                    , MAX(dt.fiscal_week_num) AS latest_wk
                    , DENSE_RANK() OVER (ORDER BY dt.fiscal_year_num DESC) AS yr_rank

                FROM
                    t2dl_das_product_funnel.product_price_funnel_daily pf
                    INNER JOIN PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW dt
                        ON pf.event_date_pacific = dt.day_date and dt.ty_ly_lly_ind in ('TY','LY')

                WHERE dt.week_end_day_date <= (SELECT MAX(event_date_pacific) FROM t2dl_das_product_funnel.product_funnel_daily)

                GROUP BY 1
            ) ty_base
        ) ty_ly
            ON dt.fiscal_year = ty_ly.fiscal_year
            --This is the join I mentioned in the subquery above
            AND dt.wk_num_realigned <= ty_ly.fiscal_week

    --Eliminate anything other than TY/LY/LLY
    WHERE ty_ly.ty_ly_ind <> 'OTHER'

) with data primary index (fiscal_year, fiscal_week) on commit preserve rows;

collect stats primary index(fiscal_year,fiscal_week), column(fiscal_week), column(fiscal_year) on dts;


-- 2. Pull Sku into Table
create multiset volatile table sku_base as (
    select
        sku.rms_sku_num
        , sku.channel_country
        , sku.web_style_num
        , sty.style_group_num
        , sku.dept_num
		, sku.prmy_supp_num AS supplier_idnt
		, sku.class_num 
		, sku.sbclass_num
        , CAST(sku.div_num AS VARCHAR(20)) || ', ' || sku.div_desc AS division
        , CAST(sku.grp_num AS VARCHAR(20)) || ', ' || sku.grp_desc AS subdiv
        , CAST(sku.dept_num AS VARCHAR(20)) || ', ' || sku.dept_desc AS department
        , CAST(sku.class_num AS VARCHAR(20)) || ', ' || sku.class_desc AS "class"
        , CAST(sku.sbclass_num AS VARCHAR(20)) || ', ' || sku.sbclass_desc AS subclass
        , UPPER(sku.brand_name) AS brand_name
        , UPPER(supp.vendor_name) AS supplier
        , UPPER(sty.type_level_1_desc) AS product_type_1
        , UPPER(sty.style_desc) AS style_desc
        , sku.partner_relationship_type_code -- MP
        , sku.npg_ind
    from prd_nap_usr_vws.product_sku_dim_vw sku
    inner join prd_nap_usr_vws.product_style_dim sty
    on sku.epm_style_num = sty.epm_style_num
      and sku.channel_country = sty.channel_country
    left join prd_nap_usr_vws.vendor_dim supp
    on sku.prmy_supp_num =supp.vendor_num
    --Exclude GWP
    where not sku.dept_num IN ('584', '585', '523') and not (sku.div_num = '340' and sku.class_num = '90')
) with data primary index(rms_sku_num, channel_country) on commit preserve rows;

collect stats primary index(rms_sku_num, channel_country), column(rms_sku_num, channel_country) on sku_base;


-- 3. Filter PFD to Relevant Dates
create multiset volatile table pfd as (
    select
          pf.rms_sku_num
          , pf.channelcountry
          , pf.channel
          , pf.platform
          , pf.web_style_num AS web_style_id
          , pf.regular_price_amt
          , pf.current_price_amt
          , pf.current_price_type
          , pf.rp_ind
          , pf.event_type
          , ty_ly.week_end_date
          --, ty_ly.week_idnt
          , ty_ly.fiscal_week
          , ty_ly.fiscal_month
          , ty_ly.fiscal_year
          , ty_ly.fiscal_quarter
          , ty_ly.ty_ly_ind
          , sum(pf.product_views) AS product_views
          , sum(pf.product_view_sessions) AS product_view_sessions
          , sum(pf.add_to_bag_quantity) AS add_to_bag_quantity
          , sum(pf.add_to_bag_sessions) AS add_to_bag_sessions
          , sum(pf.order_demand) AS order_demand
          , sum(pf.order_quantity) AS order_quantity
          , sum(pf.order_sessions) AS order_sessions
          , sum(pf.product_views*pf.pct_instock) AS instock_views
          , sum(case when pf.pct_instock is not null then pf.product_views end) AS scored_views
    from {product_funnel_t2_schema}.product_price_funnel_daily pf
    inner join dts ty_ly
        on pf.event_date_pacific = ty_ly.day_dt
    where pf.event_date_pacific >= (SELECT MIN(day_dt) FROM dts) AND pf.event_date_pacific <= (SELECT MAX(day_dt) FROM dts) 
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
) with data primary index(rms_sku_num, channelcountry, fiscal_year, fiscal_week) on commit preserve rows;

collect stats column(rms_sku_num, channelcountry), column(fiscal_year), column(fiscal_week)  on pfd;


-- 4. Join in Hierarchy
create multiset volatile table pfd_hier as (
    select
        pf.week_end_date
        --, pf.week_idnt
        , pf.fiscal_week
        , pf.fiscal_month
        , pf.fiscal_year
        , pf.fiscal_quarter
        , pf.ty_ly_ind
        , pf.channelcountry
        , pf.channel
        , pf.platform
        , sku_base.dept_num
		, sku_base.supplier_idnt
		, sku_base.class_num 
		, sku_base.sbclass_num 
        , COALESCE(sku_base.division, 'UNKNOWN') AS division
        , COALESCE(sku_base.subdiv, 'UNKNOWN') AS subdiv
        , COALESCE(sku_base.department, 'UNKNOWN') AS department
        , COALESCE(sku_base."class", 'UNKNOWN') AS "class"
        , COALESCE(sku_base.subclass, 'UNKNOWN') AS subclass
        , COALESCE(sku_base.brand_name, 'UNKNOWN') AS brand_name
        , COALESCE(sku_base.supplier, 'UNKNOWN') AS supplier
        , COALESCE(sku_base.product_type_1, 'UNKNOWN') AS product_type_1
        , pf.event_type
        , CASE WHEN rp_ind = 1 then 'RP' else 'NRP' end as rp
        , CASE WHEN partner_relationship_type_code = 'ECONCESSION' THEN 'Y' ELSE 'N' END AS mp_ind
        , sku_base.npg_ind
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
          , product_views
          , product_view_sessions
          , add_to_bag_quantity
          , add_to_bag_sessions
          , order_demand
          , order_quantity
          , order_sessions
          , instock_views
          , scored_views
    from pfd pf
    inner join sku_base
        on pf.rms_sku_num = sku_base.rms_sku_num
        and pf.channelcountry = sku_base.channel_country
) with data primary index (fiscal_year, fiscal_week, platform, brand_name) on commit preserve rows;

collect stats column(fiscal_year), column(fiscal_week), column(platform), column(brand_name)  on pfd_hier;

--ANCHOR BRANDS Lookup
--DROP TABLE anc;
CREATE MULTISET VOLATILE TABLE anc AS (
SELECT
    dept_num
    ,supplier_idnt
FROM t2dl_das_in_season_management_reporting.anchor_brands
WHERE anchor_brand_ind = 'Y'
GROUP BY 1,2
) WITH DATA PRIMARY INDEX (dept_num,supplier_idnt) ON COMMIT PRESERVE ROWS
;

COLLECT STATS PRIMARY INDEX (dept_num,supplier_idnt) ON anc ;

--RSB Lookup
--DROP TABLE rsb;
CREATE MULTISET VOLATILE TABLE rsb AS (
SELECT
    dept_num
    ,supplier_idnt
FROM t2dl_das_in_season_management_reporting.rack_strategic_brands
WHERE rack_strategic_brand_ind = 'Y'
GROUP BY 1,2
) WITH DATA PRIMARY INDEX (dept_num,supplier_idnt) ON COMMIT PRESERVE ROWS ;

COLLECT STATS PRIMARY INDEX (dept_num,supplier_idnt) ON rsb ;

-- 4-A. Join in pfd_hier, RSB, ANC and CTG   
create multiset volatile table pfd_hier2 as (
select 
    a.*,
    CASE WHEN anc.supplier_idnt IS NOT NULL THEN 1 ELSE 0 END AS anchor_brand_ind,
    CASE WHEN rsb.supplier_idnt IS NOT NULL THEN 1 ELSE 0 END AS rack_strategic_brand_ind,
    COALESCE(cat1.category, cat2.category, 'OTHER') AS category,
    COALESCE(cat1.category_group, cat2.category_group, 'OTHER') AS category_group
from pfd_hier a
left join anc
    on a.dept_num = anc.dept_num
    and a.supplier_idnt = anc.supplier_idnt
    and a.channel = 'FULL_LINE'
left join rsb
    on a.dept_num = rsb.dept_num
    and a.supplier_idnt = rsb.supplier_idnt
    and a.channel = 'RACK'
left join prd_nap_usr_vws.catg_subclass_map_dim cat1
	on a.dept_num = cat1.dept_num
	and a.class_num = cat1.class_num
	and a.sbclass_num = cat1.sbclass_num
left join prd_nap_usr_vws.catg_subclass_map_dim cat2
	on a.dept_num = cat2.dept_num
	and a.class_num = cat2.class_num
	and cat2.sbclass_num = -1
) with data primary index (fiscal_year, fiscal_week, platform, brand_name) on commit preserve rows;

collect stats column(fiscal_year), column(fiscal_week), column(platform), column(brand_name)  on pfd_hier2;

-- 5. Roll to Weekly Level & Insert into Table
insert into {product_funnel_t2_schema}.divisional_summary_weekly
    select
        week_end_date
        --, week_idnt
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
        , event_type
        , category
        , category_group
        , rp
        , mp_ind
        , npg_ind
        , anchor_brand_ind
        , rack_strategic_brand_ind
        , price_type
        , price_band_one
        , price_band_two
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
    from pfd_hier2 where fiscal_week is not null
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28
; 


collect stats column(fiscal_year), column(fiscal_week), column(platform), column(brand_name), column(subclass) on {product_funnel_t2_schema}.divisional_summary_weekly;

SET QUERY_BAND = NONE FOR SESSION;
