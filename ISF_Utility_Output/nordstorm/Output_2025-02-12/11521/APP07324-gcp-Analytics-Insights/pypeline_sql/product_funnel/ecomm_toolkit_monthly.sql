SET QUERY_BAND = 'App_ID=APP08227;
     DAG_ID=ecomm_toolkit_monthly_11521_ACE_ENG;
     Task_Name=ecomm_toolkit_monthly;'
     FOR SESSION VOLATILE;


-- FILENAME: ecomm_toolkit_monthly.sql
-- GOAL: Roll up digital product funnel to monthly level for Ecom Dashboard
-- AUTHOR: Meghan Hickey (meghan.d.hickey@nordstrom.com)


-- 1. Get this year's dates
create multiset volatile table ty_dates as (
  select
      day_dt
      , mth_454
      , mth_idnt
      , trim(yr_454 || ' ' || trim(mth_454) || ' ' || mth_abrv) as mth_label
  from prd_ma_bado.time_day_lkup_vw
  where yr_454 >= (select distinct yr_454 - 1 from prd_ma_bado.time_day_lkup_vw where day_dt = current_date)
      and mth_end_dt <= (select distinct day_dt from prd_ma_bado.time_day_lkup_vw where day_dt = current_date)
) with data primary index (day_dt) on commit preserve rows;


-- 2. Create Sku Lookup
create multiset volatile table sku_base as (
    select
        sku.rms_sku_num
        , sku.channel_country
        , sku.web_style_num
        , CAST(sku.div_num AS VARCHAR(20)) || ', ' || sku.div_desc AS div_label
        , CAST(sku.grp_num AS VARCHAR(20)) || ', ' || sku.grp_desc AS subdiv_label
        , CAST(sku.dept_num AS VARCHAR(20)) || ', ' || sku.dept_desc AS dept_label
        , CAST(sku.class_num AS VARCHAR(20)) || ', ' || sku.class_desc AS class_label
        , dept_num as dept_no
        , prmy_supp_num as supp_no
        , class_num as class_no
        , UPPER(sku.brand_name) AS brand
    from prd_nap_usr_vws.product_sku_dim_vw sku
    where sku.channel_country = 'US'
) with data primary index(rms_sku_num) on commit preserve rows;

collect stats primary index(rms_sku_num), column(rms_sku_num) on sku_base;


-- 3. Join in hierarchy for TY
create multiset volatile table ty_hier as (
  	select
    		mth_idnt
    		, mth_label
    		, phc.dept_no
    	  , phc.dept_label
    		, phc.subdiv_label
    		, phc.div_label
    		, phc.class_label
    		, phc.supp_no
    		, phc.brand
    		, phc.class_no
    		, sum(product_views_ty) AS product_views_ty
    		, sum(items_sold_ty) AS items_sold_ty
    		, sum(instock_views_ty) AS instock_views_ty
    		, sum(scored_views_ty) AS scored_views_ty
    		, sum(demand_ty) AS demand_ty

  	from (
    	  select
            mth_idnt
            , mth_label
            , rms_sku_num
        		, sum(product_views) AS product_views_ty
        		, sum(order_quantity) AS items_sold_ty
        		, sum(pct_instock * product_views) AS instock_views_ty
        		, sum(case when pct_instock is not null then product_views end) AS scored_views_ty
        		, sum(order_demand) AS demand_ty
        from t2dl_das_product_funnel.product_price_funnel_daily fct
        inner join ty_dates d
         on fct.event_date_pacific = d.day_dt
        where channel = 'FULL_LINE' and channelcountry = 'US'
        group by 1,2,3
  	) fct
		inner join sku_base phc
	  on fct.rms_sku_num = phc.rms_sku_num
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
) with data primary index(mth_idnt, dept_label, brand, class_no) on commit preserve rows;

collect stats primary index(mth_idnt, dept_label, brand, class_no), column(mth_idnt) on ty_hier;


-- 4. Join in hierarchy for LY
create multiset volatile table ly_hier as (
  	select
    		mth_idnt
    		, mth_label
    		, phc.dept_no
    	  , phc.dept_label
    		, phc.subdiv_label
    		, phc.div_label
    		, phc.class_label
    		, phc.supp_no
    		, phc.brand
    		, phc.class_no
    		, sum(product_views_ly) AS product_views_ly
    		, sum(items_sold_ly) AS items_sold_ly
    		, sum(instock_views_ly) AS instock_views_ly
    		, sum(scored_views_ly) AS scored_views_ly
    		, sum(demand_ly) AS demand_ly

  	from (
         select
            mth_idnt
            , mth_label
            , rms_sku_num
        		, sum(product_views) AS product_views_ly
        		, sum(order_quantity) AS items_sold_ly
        		, sum(pct_instock * product_views) AS instock_views_ly
        		, sum(case when pct_instock is not null then product_views end) AS scored_views_ly
        		, sum(order_demand) AS demand_ly
        from t2dl_das_product_funnel.product_price_funnel_daily fct
        inner join (
          select
              day_dt
              , mth_454
              , mth_idnt + 100 as mth_idnt
              , trim(yr_454 + 1 || ' ' || trim(mth_454) || ' ' || mth_abrv) as mth_label
          from prd_ma_bado.time_day_lkup_vw
          where yr_454 between (select distinct yr_454 from prd_ma_bado.time_day_lkup_vw where day_dt = current_date) - 2
                  and (select distinct yr_454 from prd_ma_bado.time_day_lkup_vw where day_dt = current_date) - 1
              and mth_454 in (select distinct mth_454 from ty_dates)
        ) d
         on fct.event_date_pacific = d.day_dt
        where channel = 'FULL_LINE' and channelcountry = 'US'
        group by 1,2,3
  	) fct
		inner join sku_base phc
	  on fct.rms_sku_num = phc.rms_sku_num
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
) with data primary index(mth_idnt, dept_label, brand, class_no) on commit preserve rows;

collect stats primary index(mth_idnt, dept_label, brand, class_no), column(mth_idnt) on ly_hier;


-- 5. Insert into Table
insert into {product_funnel_t2_schema}.ecomm_toolkit_monthly
    SELECT
    	COALESCE(ty.mth_idnt, ly.mth_idnt) AS mth_idnt
    	, COALESCE(ty.mth_label, ly.mth_label) AS mth_label
    	, COALESCE(ty.dept_no, ly.dept_no) AS dept_no
    	, COALESCE(ty.dept_label, ly.dept_label) AS dept_label
    	, COALESCE(ty.subdiv_label, ly.subdiv_label) AS subdiv_label
    	, COALESCE(ty.div_label, ly.div_label) AS div_label
    	, COALESCE(ty.class_label, ly.class_label) AS class_label
    	, COALESCE(ty.supp_no, ly.supp_no) AS supp_no
    	, COALESCE(ty.brand, ly.brand) AS brand
    	, COALESCE(ty.class_no, ly.class_no) AS class_no
    	, SUM(COALESCE(ty.product_views_ty, 0)) AS product_views_ty
    	, SUM(COALESCE(ty.items_sold_ty, 0)) AS items_sold_ty
    	, SUM(COALESCE(ty.instock_views_ty, 0)) AS instock_views_ty
    	, SUM(COALESCE(ty.scored_views_ty, 0)) AS scored_views_ty
    	, SUM(COALESCE(ty.demand_ty, 0)) AS web_demand_ty
    	, SUM(COALESCE(ly.product_views_ly, 0)) AS product_views_ly
    	, SUM(COALESCE(ly.items_sold_ly, 0)) AS items_sold_ly
    	, SUM(COALESCE(ly.instock_views_ly, 0)) AS instock_views_ly
    	, SUM(COALESCE(ly.scored_views_ly, 0)) AS scored_views_ly
    	, SUM(COALESCE(ly.demand_ly, 0)) AS web_demand_ly
		, CURRENT_TIMESTAMP as dw_sys_load_tmstp

    FROM ty_hier ty
    FULL OUTER JOIN ly_hier ly
    	ON ty.mth_idnt = ly.mth_idnt
    	AND ty.mth_label = ly.mth_label
    	AND ty.dept_no = ly.dept_no
    	AND ty.dept_label = ly.dept_label
    	AND ty.subdiv_label = ly.subdiv_label
    	AND ty.div_label = ly.div_label
    	AND ty.class_label = ly.class_label
    	AND ty.supp_no = ly.supp_no
    	AND ty.brand = ly.brand
    	AND ty.class_no = ly.class_no

    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
;

collect stats primary index(mth_idnt ,dept_label ,brand ,class_no)
    , column(mth_idnt)
    , column(dept_label)
    , column(brand)
    , column(class_no) on {product_funnel_t2_schema}.ecomm_toolkit_monthly;

SET QUERY_BAND = NONE FOR SESSION;