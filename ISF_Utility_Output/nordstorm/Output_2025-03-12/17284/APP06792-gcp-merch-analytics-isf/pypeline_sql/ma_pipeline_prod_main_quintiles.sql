-- Pull TY/LY YTD
create multiset volatile table date_lkp as (
    SELECT
        DISTINCT day_date
        , week_idnt
        , month_idnt
        , quarter_idnt
    FROM PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW
    WHERE month_end_day_date <= current_date
        and ty_ly_lly_ind in ('TY', 'LY')
) with data primary index(day_date, week_idnt, month_idnt) on commit preserve rows;

--Create sku look-up table
create multiset volatile table sku_base as (
    SELECT
        DISTINCT sku.rms_sku_num
        , supp_part_num || ', ' || supp_color as cc
        , supp_part_num as vpn
        , supp_color as supplier_color
        , style_group_desc
        , style_group_num || ', ' || style_group_desc as sg_desc
        , sku.style_group_num
        , CAST(sku.div_num AS VARCHAR(20)) || ', ' || sku.div_desc AS division
        , CAST(sku.grp_num AS VARCHAR(20)) || ', ' || sku.grp_desc AS subdivision
        , CAST(sku.dept_num AS VARCHAR(20)) || ', ' || sku.dept_desc AS department
        , CAST(sku.class_num AS VARCHAR(20)) || ', ' || sku.class_desc AS "class"
        , CAST(sku.sbclass_num AS VARCHAR(20)) || ', ' || sku.sbclass_desc AS subclass
        , UPPER(sku.brand_name) AS brand_name
        , UPPER(supp.vendor_name) as supplier
        , coalesce(thm1.category, thm2.category) as category
        , npg_ind as npg_flag
    FROM prd_nap_usr_vws.product_sku_dim_vw sku
    LEFT JOIN prd_nap_usr_vws.vendor_dim supp
        ON sku.prmy_supp_num =supp.vendor_num
    LEFT JOIN prd_nap_usr_vws.catg_subclass_map_dim thm1
    		ON sku.dept_num = thm1.dept_num
    		AND sku.class_num = thm1.class_num
    		AND sku.sbclass_num = thm1.sbclass_num
    LEFT JOIN prd_nap_usr_vws.catg_subclass_map_dim thm2
    		ON sku.dept_num = thm2.dept_num
    		AND sku.class_num = thm2.class_num
    		AND thm2.sbclass_num <> 174
    WHERE channel_country = 'US'
      AND NOT sku.dept_num IN ('584', '585', '523') AND NOT (sku.div_num = '340' AND sku.class_num = '90')
      AND brand_name <> 'NQC'
) with data primary index(rms_sku_num) on commit preserve rows;


-- Create RP indicator look-up
create multiset volatile table rp_lkp
  as (
      select
          month_idnt
          , trim(channel_num || ', ' || channel_desc) as channel
          , cc
          , supplier
          , department
          , "class"
      from PRD_NAP_USR_VWS.MERCH_RP_SKU_LOC_DIM_HIST rp
      inner join (
                select distinct day_date, month_idnt
                from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW
                where day_date between (select min(day_date) from date_lkp) and (select max(day_date) from date_lkp)
            ) d
        on d.day_date between BEGIN(rp_period) and END(rp_period)
      inner join prd_nap_usr_vws.store_dim st
        on rp.location_num = st.store_num
      inner join sku_base s
        on rp.rms_sku_num = s.rms_sku_num
      where business_unit_desc in ('N.COM', 'OFFPRICE ONLINE')
      group by 1,2,3,4,5,6
) with data primary index (month_idnt, channel, cc) on commit preserve rows;


--N.com Sales/EOH
create multiset volatile table sales_eoh_ncom as (
    select
        quarter_idnt
        , month_idnt
        , cc
        , trim(channel_num || ', ' || channel_desc) as channel
        , division
        , subdivision
        , department
        , "class"
        , subclass
        , brand_name
        , supplier
        , category
        , sg_desc
        , style_group_num
        , vpn
        , supplier_color
        , style_group_desc
        , npg_flag

        , sum(sales_dollars) as sales_r
        , sum(sales_units) as sales_u
        , sum(cost_of_goods_sold) as sales_c
        , sum(case when price_type = 'R' then sales_dollars else 0 end) as sales_r_reg
        , sum(case when price_type = 'P' then sales_dollars else 0 end) as sales_r_pro
        , sum(case when price_type = 'C' then sales_dollars else 0 end) as sales_r_clr
        , sum(case when price_type = 'R' then sales_units else 0 end) as sales_u_reg
        , sum(case when price_type = 'P' then sales_units else 0 end) as sales_u_pro
        , sum(case when price_type = 'C' then sales_units else 0 end) as sales_u_clr
        , sum(demand_dollars) as demand_r
        , sum(demand_units) as demand_u
        , sum(case when price_type = 'R' then demand_dollars else 0 end) as demand_r_reg
        , sum(case when price_type = 'P' then demand_dollars else 0 end) as demand_r_pro
        , sum(case when price_type = 'C' then demand_dollars else 0 end) as demand_r_clr
        , sum(case when price_type = 'R' then demand_units else 0 end) as demand_u_reg
        , sum(case when price_type = 'P' then demand_units else 0 end) as demand_u_pro
        , sum(case when price_type = 'C' then demand_units else 0 end) as demand_u_clr
        , sum(return_dollars) as returns_r
        , sum(return_units) as returns_u
        , sum(case when day_dt = month_end_day_date then eoh_units else 0 end) AS eoh_u
        , sum(case when day_dt = month_end_day_date then eoh_dollars else 0 end) AS eoh_r
        , sum(case when day_dt = month_end_day_date then eoh_units * weighted_average_cost else 0 end) AS eoh_c
        , sum(case when price_type in ('P', 'R') and day_dt = month_end_day_date then eoh_units else 0 end) AS eoh_u_regpro
        , sum(case when price_type in ('C') and day_dt = month_end_day_date then eoh_units else 0 end) AS eoh_u_clr

        , cast(0 as decimal(12,2)) as receipts_r
        , cast(0 as decimal(10,0)) as receipts_u
        , cast(0 as decimal(12,2)) as receipts_c
        , cast(0 as decimal(12,2)) as dropship_receipts_r
        , cast(0 as decimal(10,0)) as dropship_receipts_u

        , cast(0 as decimal(12,2)) AS product_views
        , cast(0 as decimal(12,2)) AS add_to_bag_quantity
        , cast(0 as decimal(12,2)) AS order_quantity
        , cast(0 as decimal(12,2)) AS instock_views
        , cast(0 as decimal(12,2)) AS scored_views
        , cast(0 as decimal(12,2)) as oos_views
        , cast(0 as decimal(12,2)) AS product_views_reg
        , cast(0 as decimal(12,2)) AS instock_views_reg
        , cast(0 as decimal(12,2)) AS scored_views_reg

        , cast(0 as decimal(10,0)) as pah_r
        , cast(0 as decimal(10,0)) as pah_u
        , cast(0 as decimal(10,0)) as pah_c

        , max(case when regular_price is null then '11. Unknown'
                when regular_price <= 10.00     then '1. < $10'
                when regular_price <= 25.00     THEN '2. $10 - $25'
                when regular_price <= 50.00     THEN '3. $25 - $50'
                when regular_price <= 100.00    THEN '4. $50 - $100'
                when regular_price <= 150.00    THEN '5. $100 - $150'
                when regular_price <= 200.00    THEN '6. $150 - $200'
                when regular_price <= 300.00    THEN '7. $200 - $300'
                when regular_price <= 500.00    THEN '8. $300 - $500'
                when regular_price <= 1000.00   THEN '9. $500 - $1000'
                when regular_price >  1000.00   THEN '10. > $1000'
              end) as price_band_aur
        , max(case when regular_price is null then 'Unknown'
                when regular_price <= 10.00     THEN '< $10'
                when regular_price <= 15.00     THEN '$10 - $15'
                when regular_price <= 20.00     THEN '$15 - $20'
                when regular_price <= 25.00     THEN '$20 - $25'
                when regular_price <= 30.00     THEN '$25 - $30'
                when regular_price <= 40.00     THEN '$30 - $40'
                when regular_price <= 50.00     THEN '$40 - $50'
                when regular_price <= 60.00     THEN '$50 - $60'
                when regular_price <= 80.00     THEN '$60 - $80'
                when regular_price <= 100.00    THEN '$80 - $100'
                when regular_price <= 125.00    THEN '$100 - $125'
                when regular_price <= 150.00    THEN '$125 - $150'
                when regular_price <= 175.00    THEN '$150 - $175'
                when regular_price <= 200.00    THEN '$175 - $200'
                when regular_price <= 250.00    THEN '$200 - $250'
                when regular_price <= 300.00    THEN '$250 - $300'
                when regular_price <= 400.00    THEN '$300 - $400'
                when regular_price <= 500.00    THEN '$400 - $500'
                when regular_price <= 700.00    THEN '$500 - $700'
                when regular_price <= 900.00    THEN '$700 - $900'
                when regular_price <= 1000.00   THEN '$900 - $1000'
                when regular_price <= 1200.00   THEN '$1000 - $1200'
                when regular_price <= 1500.00   THEN '$1200 - $1500'
                when regular_price <= 1800.00   THEN '$1500 - $1800'
                when regular_price <= 2000.00   THEN '$1800 - $2000'
                when regular_price <= 3000.00   THEN '$2000 - $3000'
                when regular_price <= 4000.00   THEN '$3000 - $4000'
                when regular_price <= 5000.00   THEN '$4000 - $5000'
                when regular_price >  5000.00   THEN '> $5000'
                end) as price_band_two_aur

    from t2dl_das_ace_mfp.sku_loc_pricetype_day_vw a
    inner join prd_nap_usr_vws.store_dim b
      on a.loc_idnt = b.store_num
      and b.channel_num in (120)
    inner join PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW c
      on a.day_dt = c.day_date
    inner join sku_base d
      on a.sku_idnt = d.rms_sku_num
    where day_date between (select min(day_date) from date_lkp) and (select max(day_date) from date_lkp)
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
) with data primary index (quarter_idnt, month_idnt, cc, channel) on commit preserve rows
;

-- R.com Sales/EOH
create multiset volatile table sales_eoh_rcom as (
    select
        quarter_idnt
        , month_idnt
        , cc
        , trim(channel_num || ', ' || channel_desc) as channel
        , division
        , subdivision
        , department
        , "class"
        , subclass
        , brand_name
        , supplier
        , category
        , sg_desc
        , style_group_num
        , vpn
        , supplier_color
        , style_group_desc
        , npg_flag

        , sum(sales_dollars) as sales_r
        , sum(sales_units) as sales_u
        , sum(cost_of_goods_sold) as sales_c
        , sum(case when price_type = 'R' then sales_dollars else 0 end) as sales_r_reg
        , sum(case when price_type = 'P' then sales_dollars else 0 end) as sales_r_pro
        , sum(case when price_type = 'C' then sales_dollars else 0 end) as sales_r_clr
        , sum(case when price_type = 'R' then sales_units else 0 end) as sales_u_reg
        , sum(case when price_type = 'P' then sales_units else 0 end) as sales_u_pro
        , sum(case when price_type = 'C' then sales_units else 0 end) as sales_u_clr
        , sum(demand_dollars) as demand_r
        , sum(demand_units) as demand_u
        , sum(case when price_type = 'R' then demand_dollars else 0 end) as demand_r_reg
        , sum(case when price_type = 'P' then demand_dollars else 0 end) as demand_r_pro
        , sum(case when price_type = 'C' then demand_dollars else 0 end) as demand_r_clr
        , sum(case when price_type = 'R' then demand_units else 0 end) as demand_u_reg
        , sum(case when price_type = 'P' then demand_units else 0 end) as demand_u_pro
        , sum(case when price_type = 'C' then demand_units else 0 end) as demand_u_clr
        , sum(return_dollars) as returns_r
        , sum(return_units) as returns_u
        , sum(case when day_dt = month_end_day_date then eoh_units else 0 end) AS eoh_u
        , sum(case when day_dt = month_end_day_date then eoh_dollars else 0 end) AS eoh_r
        , sum(case when day_dt = month_end_day_date then eoh_units * weighted_average_cost else 0 end) AS eoh_c
        , sum(case when price_type in ('P', 'R') and day_dt = month_end_day_date then eoh_units else 0 end) AS eoh_u_regpro
        , sum(case when price_type in ('C') and day_dt = month_end_day_date then eoh_units else 0 end) AS eoh_u_clr

        , cast(0 as decimal(12,2)) as receipts_r
        , cast(0 as decimal(10,0)) as receipts_u
        , cast(0 as decimal(12,2)) as receipts_c
        , cast(0 as decimal(12,2)) as dropship_receipts_r
        , cast(0 as decimal(10,0)) as dropship_receipts_u

        , cast(0 as decimal(12,2)) AS product_views
        , cast(0 as decimal(12,2)) AS add_to_bag_quantity
        , cast(0 as decimal(12,2)) AS order_quantity
        , cast(0 as decimal(12,2)) AS instock_views
        , cast(0 as decimal(12,2)) AS scored_views
        , cast(0 as decimal(12,2)) as oos_views
        , cast(0 as decimal(12,2)) AS product_views_reg
        , cast(0 as decimal(12,2)) AS instock_views_reg
        , cast(0 as decimal(12,2)) AS scored_views_reg

        , cast(0 as decimal(10,0)) as pah_r
        , cast(0 as decimal(10,0)) as pah_u
        , cast(0 as decimal(10,0)) as pah_c

        , max(case when regular_price is null then '11. Unknown'
                when regular_price <= 10.00     then '1. < $10'
                when regular_price <= 25.00     THEN '2. $10 - $25'
                when regular_price <= 50.00     THEN '3. $25 - $50'
                when regular_price <= 100.00    THEN '4. $50 - $100'
                when regular_price <= 150.00    THEN '5. $100 - $150'
                when regular_price <= 200.00    THEN '6. $150 - $200'
                when regular_price <= 300.00    THEN '7. $200 - $300'
                when regular_price <= 500.00    THEN '8. $300 - $500'
                when regular_price <= 1000.00   THEN '9. $500 - $1000'
                when regular_price >  1000.00   THEN '10. > $1000'
              end) as price_band_aur
        , max(case when regular_price is null then 'Unknown'
                when regular_price <= 10.00     THEN '< $10'
                when regular_price <= 15.00     THEN '$10 - $15'
                when regular_price <= 20.00     THEN '$15 - $20'
                when regular_price <= 25.00     THEN '$20 - $25'
                when regular_price <= 30.00     THEN '$25 - $30'
                when regular_price <= 40.00     THEN '$30 - $40'
                when regular_price <= 50.00     THEN '$40 - $50'
                when regular_price <= 60.00     THEN '$50 - $60'
                when regular_price <= 80.00     THEN '$60 - $80'
                when regular_price <= 100.00    THEN '$80 - $100'
                when regular_price <= 125.00    THEN '$100 - $125'
                when regular_price <= 150.00    THEN '$125 - $150'
                when regular_price <= 175.00    THEN '$150 - $175'
                when regular_price <= 200.00    THEN '$175 - $200'
                when regular_price <= 250.00    THEN '$200 - $250'
                when regular_price <= 300.00    THEN '$250 - $300'
                when regular_price <= 400.00    THEN '$300 - $400'
                when regular_price <= 500.00    THEN '$400 - $500'
                when regular_price <= 700.00    THEN '$500 - $700'
                when regular_price <= 900.00    THEN '$700 - $900'
                when regular_price <= 1000.00   THEN '$900 - $1000'
                when regular_price <= 1200.00   THEN '$1000 - $1200'
                when regular_price <= 1500.00   THEN '$1200 - $1500'
                when regular_price <= 1800.00   THEN '$1500 - $1800'
                when regular_price <= 2000.00   THEN '$1800 - $2000'
                when regular_price <= 3000.00   THEN '$2000 - $3000'
                when regular_price <= 4000.00   THEN '$3000 - $4000'
                when regular_price <= 5000.00   THEN '$4000 - $5000'
                when regular_price >  5000.00   THEN '> $5000'
                end) as price_band_two_aur

    from t2dl_das_ace_mfp.sku_loc_pricetype_day_vw a
    inner join prd_nap_usr_vws.store_dim b
      on a.loc_idnt = b.store_num
      and b.channel_num in (250)
    inner join PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW c
      on a.day_dt = c.day_date
    inner join sku_base d
      on a.sku_idnt = d.rms_sku_num
    where day_date between (select min(day_date) from date_lkp) and (select max(day_date) from date_lkp)
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
) with data primary index (quarter_idnt, month_idnt, cc, channel) on commit preserve rows
;

--- Receipts
create multiset volatile table rcpts as (
    select
        quarter_idnt
        , month_idnt
        , cc
        , trim(b.channel_num || ', ' || b.channel_desc) as channel
        , division
        , subdivision
        , department
        , "class"
        , subclass
        , brand_name
        , supplier
        , category
        , sg_desc
        , style_group_num
        , vpn
        , supplier_color
        , style_group_desc
        , npg_flag

        , cast(0 as decimal(12,2)) as sales_r
        , cast(0 as decimal(10,0)) as sales_u
        , cast(0 as decimal(12,2)) as sales_c
        , cast(0 as decimal(12,2)) as sales_r_reg
        , cast(0 as decimal(12,2)) as sales_r_pro
        , cast(0 as decimal(12,2)) as sales_r_clr
        , cast(0 as decimal(10,0)) as sales_u_reg
        , cast(0 as decimal(10,0)) as sales_u_pro
        , cast(0 as decimal(10,0)) as sales_u_clr
        , cast(0 as decimal(12,2)) as demand_r
        , cast(0 as decimal(10,0)) as demand_u
        , cast(0 as decimal(12,2)) as demand_r_reg
        , cast(0 as decimal(12,2)) as demand_r_pro
        , cast(0 as decimal(12,2)) as demand_r_clr
        , cast(0 as decimal(10,0)) as demand_u_reg
        , cast(0 as decimal(10,0)) as demand_u_pro
        , cast(0 as decimal(10,0)) as sdemand_u_clr
        , cast(0 as decimal(12,2)) as returns_r
        , cast(0 as decimal(10,0)) as returns_u
        , cast(0 as decimal(10,0)) as eoh_u
        , cast(0 as decimal(12,2)) as eoh_r
        , cast(0 as decimal(12,2)) as eoh_c
        , cast(0 as decimal(10,0)) as eoh_u_regpro
        , cast(0 as decimal(10,0)) as eoh_u_clr

        , sum(coalesce(receipts_regular_retail,0) + coalesce(receipts_clearance_retail,0) + coalesce(receipts_crossdock_regular_retail,0) + coalesce(receipts_crossdock_clearance_retail,0)) as receipts_r
        , sum(coalesce(receipts_regular_units,0) + coalesce(receipts_clearance_units,0) + coalesce(receipts_crossdock_regular_units,0) + coalesce(receipts_crossdock_clearance_units,0)) as receipts_u
        , sum(coalesce(receipts_regular_cost,0) + coalesce(receipts_clearance_cost,0) + coalesce(receipts_crossdock_regular_cost,0) + coalesce(receipts_crossdock_clearance_cost,0)) as receipts_c
        , sum(case when dropship_ind = 'Y' then coalesce(receipts_regular_retail,0) + coalesce(receipts_clearance_retail,0) + coalesce(receipts_crossdock_regular_retail,0) + coalesce(receipts_crossdock_clearance_retail,0) else 0 end) as dropship_receipts_r
        , sum(case when dropship_ind = 'Y' then coalesce(receipts_regular_units,0) + coalesce(receipts_clearance_units,0) + coalesce(receipts_crossdock_regular_units,0) + coalesce(receipts_crossdock_clearance_units,0) else 0 end) as dropship_receipts_u

        , cast(0 as decimal(12,2)) AS product_views
        , cast(0 as decimal(12,2)) AS add_to_bag_quantity
        , cast(0 as decimal(12,2)) AS order_quantity
        , cast(0 as decimal(12,2)) AS instock_views
        , cast(0 as decimal(12,2)) AS scored_views
        , cast(0 as decimal(12,2)) as oos_views
        , cast(0 as decimal(12,2)) AS product_views_reg
        , cast(0 as decimal(12,2)) AS instock_views_reg
        , cast(0 as decimal(12,2)) AS scored_views_reg

        , cast(0 as decimal(10,0)) as pah_r
        , cast(0 as decimal(10,0)) as pah_u
        , cast(0 as decimal(10,0)) as pah_c

        , case when receipts_u is null or receipts_u = 0 then '11. Unknown'
                when receipts_r/receipts_u <= 10.00     then '1. < $10'
                when receipts_r/receipts_u <= 25.00     THEN '2. $10 - $25'
                when receipts_r/receipts_u <= 50.00     THEN '3. $25 - $50'
                when receipts_r/receipts_u <= 100.00    THEN '4. $50 - $100'
                when receipts_r/receipts_u <= 150.00    THEN '5. $100 - $150'
                when receipts_r/receipts_u <= 200.00    THEN '6. $150 - $200'
                when receipts_r/receipts_u <= 300.00    THEN '7. $200 - $300'
                when receipts_r/receipts_u <= 500.00    THEN '8. $300 - $500'
                when receipts_r/receipts_u <= 1000.00   THEN '9. $500 - $1000'
                when receipts_r/receipts_u >  1000.00   THEN '10. > $1000'
              end as price_band_aur
        , case when receipts_u is null or receipts_u = 0 then 'Unknown'
                when receipts_r/receipts_u <= 10.00     THEN '< $10'
                when receipts_r/receipts_u <= 15.00     THEN '$10 - $15'
                when receipts_r/receipts_u <= 20.00     THEN '$15 - $20'
                when receipts_r/receipts_u <= 25.00     THEN '$20 - $25'
                when receipts_r/receipts_u <= 30.00     THEN '$25 - $30'
                when receipts_r/receipts_u <= 40.00     THEN '$30 - $40'
                when receipts_r/receipts_u <= 50.00     THEN '$40 - $50'
                when receipts_r/receipts_u <= 60.00     THEN '$50 - $60'
                when receipts_r/receipts_u <= 80.00     THEN '$60 - $80'
                when receipts_r/receipts_u <= 100.00    THEN '$80 - $100'
                when receipts_r/receipts_u <= 125.00    THEN '$100 - $125'
                when receipts_r/receipts_u <= 150.00    THEN '$125 - $150'
                when receipts_r/receipts_u <= 175.00    THEN '$150 - $175'
                when receipts_r/receipts_u <= 200.00    THEN '$175 - $200'
                when receipts_r/receipts_u <= 250.00    THEN '$200 - $250'
                when receipts_r/receipts_u <= 300.00    THEN '$250 - $300'
                when receipts_r/receipts_u <= 400.00    THEN '$300 - $400'
                when receipts_r/receipts_u <= 500.00    THEN '$400 - $500'
                when receipts_r/receipts_u <= 700.00    THEN '$500 - $700'
                when receipts_r/receipts_u <= 900.00    THEN '$700 - $900'
                when receipts_r/receipts_u <= 1000.00   THEN '$900 - $1000'
                when receipts_r/receipts_u <= 1200.00   THEN '$1000 - $1200'
                when receipts_r/receipts_u <= 1500.00   THEN '$1200 - $1500'
                when receipts_r/receipts_u <= 1800.00   THEN '$1500 - $1800'
                when receipts_r/receipts_u <= 2000.00   THEN '$1800 - $2000'
                when receipts_r/receipts_u <= 3000.00   THEN '$2000 - $3000'
                when receipts_r/receipts_u <= 4000.00   THEN '$3000 - $4000'
                when receipts_r/receipts_u <= 5000.00   THEN '$4000 - $5000'
                when receipts_r/receipts_u >  5000.00   THEN '> $5000'
              end as price_band_two_aur
    from prd_nap_usr_vws.merch_poreceipt_sku_store_week_fact_vw  a
    inner join prd_nap_usr_vws.store_dim b
      on a.store_num = b.store_num
    inner join (
        select distinct week_idnt, month_idnt, quarter_idnt
        from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW
        where day_date between (select min(day_date) - 7*13 from date_lkp) and (select max(day_date) from date_lkp)
     ) c
      on a.week_num = c.week_idnt
    inner join sku_base d
      on a.rms_sku_num = d.rms_sku_num
    where a.channel_num in (120,250)
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
) with data primary index (quarter_idnt, month_idnt, cc, channel) on commit preserve rows
;


---Pack & Hold Transfers In
create multiset volatile table pah as (
    select
        quarter_idnt
        , month_idnt
        , cc
        , trim(b.channel_num || ', ' || b.channel_desc) as channel
        , division
        , subdivision
        , department
        , "class"
        , subclass
        , brand_name
        , supplier
        , category
        , sg_desc
        , style_group_num
        , vpn
        , supplier_color
        , style_group_desc
        , npg_flag

        , cast(0 as decimal(12,2)) as sales_r
        , cast(0 as decimal(10,0)) as sales_u
        , cast(0 as decimal(12,2)) as sales_c
        , cast(0 as decimal(12,2)) as sales_r_reg
        , cast(0 as decimal(12,2)) as sales_r_pro
        , cast(0 as decimal(12,2)) as sales_r_clr
        , cast(0 as decimal(10,0)) as sales_u_reg
        , cast(0 as decimal(10,0)) as sales_u_pro
        , cast(0 as decimal(10,0)) as sales_u_clr
        , cast(0 as decimal(12,2)) as demand_r
        , cast(0 as decimal(10,0)) as demand_u
        , cast(0 as decimal(12,2)) as demand_r_reg
        , cast(0 as decimal(12,2)) as demand_r_pro
        , cast(0 as decimal(12,2)) as demand_r_clr
        , cast(0 as decimal(10,0)) as demand_u_reg
        , cast(0 as decimal(10,0)) as demand_u_pro
        , cast(0 as decimal(10,0)) as sdemand_u_clr
        , cast(0 as decimal(12,2)) as returns_r
        , cast(0 as decimal(10,0)) as returns_u
        , cast(0 as decimal(10,0)) as eoh_u
        , cast(0 as decimal(12,2)) as eoh_r
        , cast(0 as decimal(12,2)) as eoh_c
        , cast(0 as decimal(10,0)) as eoh_u_regpro
        , cast(0 as decimal(10,0)) as eoh_u_clr

        , cast(0 as decimal(10,0)) as receipts_r
        , cast(0 as decimal(10,0))as receipts_u
        , cast(0 as decimal(10,0)) as receipts_c
        , cast(0 as decimal(10,0)) as dropship_receipts_r
        , cast(0 as decimal(10,0)) as dropship_receipts_u

        , cast(0 as decimal(12,2)) AS product_views
        , cast(0 as decimal(12,2)) AS add_to_bag_quantity
        , cast(0 as decimal(12,2)) AS order_quantity
        , cast(0 as decimal(12,2)) AS instock_views
        , cast(0 as decimal(12,2)) AS scored_views
        , cast(0 as decimal(12,2)) as oos_views
        , cast(0 as decimal(12,2)) AS product_views_reg
        , cast(0 as decimal(12,2)) AS instock_views_reg
        , cast(0 as decimal(12,2)) AS scored_views_reg

        , sum(packandhold_transfer_in_retail) as pah_r
        , sum(packandhold_transfer_in_units) as pah_u
        , sum(packandhold_transfer_in_cost) as pah_c

        , case when pah_u is null or pah_u = 0 then '11. Unknown'
                when pah_r/pah_u <= 10.00     then '1. < $10'
                when pah_r/pah_u <= 25.00     THEN '2. $10 - $25'
                when pah_r/pah_u <= 50.00     THEN '3. $25 - $50'
                when pah_r/pah_u <= 100.00    THEN '4. $50 - $100'
                when pah_r/pah_u <= 150.00    THEN '5. $100 - $150'
                when pah_r/pah_u <= 200.00    THEN '6. $150 - $200'
                when pah_r/pah_u <= 300.00    THEN '7. $200 - $300'
                when pah_r/pah_u <= 500.00    THEN '8. $300 - $500'
                when pah_r/pah_u <= 1000.00   THEN '9. $500 - $1000'
                when pah_r/pah_u >  1000.00   THEN '10. > $1000'
              end as price_band_aur
        , case when pah_u is null or pah_u = 0 then 'Unknown'
                when pah_r/pah_u <= 10.00     THEN '< $10'
                when pah_r/pah_u <= 15.00     THEN '$10 - $15'
                when pah_r/pah_u <= 20.00     THEN '$15 - $20'
                when pah_r/pah_u <= 25.00     THEN '$20 - $25'
                when pah_r/pah_u <= 30.00     THEN '$25 - $30'
                when pah_r/pah_u <= 40.00     THEN '$30 - $40'
                when pah_r/pah_u <= 50.00     THEN '$40 - $50'
                when pah_r/pah_u <= 60.00     THEN '$50 - $60'
                when pah_r/pah_u <= 80.00     THEN '$60 - $80'
                when pah_r/pah_u <= 100.00    THEN '$80 - $100'
                when pah_r/pah_u <= 125.00    THEN '$100 - $125'
                when pah_r/pah_u <= 150.00    THEN '$125 - $150'
                when pah_r/pah_u <= 175.00    THEN '$150 - $175'
                when pah_r/pah_u <= 200.00    THEN '$175 - $200'
                when pah_r/pah_u <= 250.00    THEN '$200 - $250'
                when pah_r/pah_u <= 300.00    THEN '$250 - $300'
                when pah_r/pah_u <= 400.00    THEN '$300 - $400'
                when pah_r/pah_u <= 500.00    THEN '$400 - $500'
                when pah_r/pah_u <= 700.00    THEN '$500 - $700'
                when pah_r/pah_u <= 900.00    THEN '$700 - $900'
                when pah_r/pah_u <= 1000.00   THEN '$900 - $1000'
                when pah_r/pah_u <= 1200.00   THEN '$1000 - $1200'
                when pah_r/pah_u <= 1500.00   THEN '$1200 - $1500'
                when pah_r/pah_u <= 1800.00   THEN '$1500 - $1800'
                when pah_r/pah_u <= 2000.00   THEN '$1800 - $2000'
                when pah_r/pah_u <= 3000.00   THEN '$2000 - $3000'
                when pah_r/pah_u <= 4000.00   THEN '$3000 - $4000'
                when pah_r/pah_u <= 5000.00   THEN '$4000 - $5000'
                when pah_r/pah_u >  5000.00   THEN '> $5000'
              end as price_band_two_aur
    from prd_nap_usr_vws.merch_transfer_sku_loc_week_agg_fact_vw  a
    inner join prd_nap_usr_vws.store_dim b
      on a.store_num = b.store_num
    inner join (
        select distinct week_idnt, month_idnt, quarter_idnt
        from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW
        where day_date between (select min(day_date) - 7*13 from date_lkp) and (select max(day_date) from date_lkp)
     ) c
      on a.week_num = c.week_idnt
    inner join sku_base d
      on a.rms_sku_num = d.rms_sku_num
    where a.channel_num in (120,250) and a.packandhold_transfer_in_units <> 0
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
) with data primary index (quarter_idnt, month_idnt, cc, channel) on commit preserve rows
;

--- PFD
create multiset volatile table pfd as (
    select
        quarter_idnt
        , month_idnt
        , cc
        , channel
        , division
        , subdivision
        , department
        , "class"
        , subclass
        , brand_name
        , supplier
        , category
        , sg_desc
        , style_group_num
        , vpn
        , supplier_color
        , style_group_desc
        , npg_flag

        , cast(0 as decimal(12,2)) as sales_r
        , cast(0 as decimal(10,0)) as sales_u
        , cast(0 as decimal(12,2)) as sales_c
        , cast(0 as decimal(12,2)) as sales_r_reg
        , cast(0 as decimal(12,2)) as sales_r_pro
        , cast(0 as decimal(12,2)) as sales_r_clr
        , cast(0 as decimal(10,0)) as sales_u_reg
        , cast(0 as decimal(10,0)) as sales_u_pro
        , cast(0 as decimal(10,0)) as sales_u_clr
        , cast(0 as decimal(12,2)) as demand_r
        , cast(0 as decimal(10,0)) as demand_u
        , cast(0 as decimal(12,2)) as demand_r_reg
        , cast(0 as decimal(12,2)) as demand_r_pro
        , cast(0 as decimal(12,2)) as demand_r_clr
        , cast(0 as decimal(10,0)) as demand_u_reg
        , cast(0 as decimal(10,0)) as demand_u_pro
        , cast(0 as decimal(10,0)) as sdemand_u_clr
        , cast(0 as decimal(12,2)) as returns_r
        , cast(0 as decimal(10,0)) as returns_u
        , cast(0 as decimal(10,0)) as eoh_u
        , cast(0 as decimal(12,2)) as eoh_r
        , cast(0 as decimal(12,2)) as eoh_c
        , cast(0 as decimal(10,0)) as eoh_u_regpro
        , cast(0 as decimal(10,0)) as eoh_u_clr

        , cast(0 as decimal(12,2)) as receipts_r
        , cast(0 as decimal(10,0)) as receipts_u
        , cast(0 as decimal(12,2)) as receipts_c
        , cast(0 as decimal(12,2)) as dropship_receipts_r
        , cast(0 as decimal(10,0)) as dropship_receipts_u

        , sum(product_views) AS product_views
        , sum(add_to_bag_quantity) AS add_to_bag_quantity
        , sum(order_quantity) AS order_quantity
        , sum(instock_views) AS instock_views
        , sum(scored_views) AS scored_views
        , sum(scored_views) - sum(instock_views) as oos_views
        , sum(product_views_reg) AS product_views_reg
        , sum(instock_views_reg) AS instock_views_reg
        , sum(scored_views_reg) AS scored_views_reg

        , cast(0 as decimal(10,0)) as pah_r
        , cast(0 as decimal(10,0)) as pah_u
        , cast(0 as decimal(10,0)) as pah_c

        , max(case when regular_price_amt is null then '11. Unknown'
                when regular_price_amt <= 10.00     then '1. < $10'
                when regular_price_amt <= 25.00     THEN '2. $10 - $25'
                when regular_price_amt <= 50.00     THEN '3. $25 - $50'
                when regular_price_amt <= 100.00    THEN '4. $50 - $100'
                when regular_price_amt <= 150.00    THEN '5. $100 - $150'
                when regular_price_amt <= 200.00    THEN '6. $150 - $200'
                when regular_price_amt <= 300.00    THEN '7. $200 - $300'
                when regular_price_amt <= 500.00    THEN '8. $300 - $500'
                when regular_price_amt <= 1000.00   THEN '9. $500 - $1000'
                when regular_price_amt >  1000.00   THEN '10. > $1000'
              end) as price_band_aur
        , max(case when regular_price_amt is null then 'Unknown'
                when regular_price_amt <= 10.00     THEN '< $10'
                when regular_price_amt <= 15.00     THEN '$10 - $15'
                when regular_price_amt <= 20.00     THEN '$15 - $20'
                when regular_price_amt <= 25.00     THEN '$20 - $25'
                when regular_price_amt <= 30.00     THEN '$25 - $30'
                when regular_price_amt <= 40.00     THEN '$30 - $40'
                when regular_price_amt <= 50.00     THEN '$40 - $50'
                when regular_price_amt <= 60.00     THEN '$50 - $60'
                when regular_price_amt <= 80.00     THEN '$60 - $80'
                when regular_price_amt <= 100.00    THEN '$80 - $100'
                when regular_price_amt <= 125.00    THEN '$100 - $125'
                when regular_price_amt <= 150.00    THEN '$125 - $150'
                when regular_price_amt <= 175.00    THEN '$150 - $175'
                when regular_price_amt <= 200.00    THEN '$175 - $200'
                when regular_price_amt <= 250.00    THEN '$200 - $250'
                when regular_price_amt <= 300.00    THEN '$250 - $300'
                when regular_price_amt <= 400.00    THEN '$300 - $400'
                when regular_price_amt <= 500.00    THEN '$400 - $500'
                when regular_price_amt <= 700.00    THEN '$500 - $700'
                when regular_price_amt <= 900.00    THEN '$700 - $900'
                when regular_price_amt <= 1000.00   THEN '$900 - $1000'
                when regular_price_amt <= 1200.00   THEN '$1000 - $1200'
                when regular_price_amt <= 1500.00   THEN '$1200 - $1500'
                when regular_price_amt <= 1800.00   THEN '$1500 - $1800'
                when regular_price_amt <= 2000.00   THEN '$1800 - $2000'
                when regular_price_amt <= 3000.00   THEN '$2000 - $3000'
                when regular_price_amt <= 4000.00   THEN '$3000 - $4000'
                when regular_price_amt <= 5000.00   THEN '$4000 - $5000'
                when regular_price_amt >  5000.00   THEN '> $5000'
              end) as price_band_two_aur
    from (
        select
            quarter_idnt
            , month_idnt
            , rms_sku_num
            , regular_price_amt
            , case when channel = 'FULL_LINE' then '120, N.COM' else '250, OFFPRICE ONLINE' end as channel
            , sum(product_views) AS product_views
            , sum(add_to_bag_quantity) AS add_to_bag_quantity
            , sum(order_quantity) AS order_quantity
            , sum(product_views*pct_instock) AS instock_views
            , sum(case when pct_instock is not null then product_views end) AS scored_views
            , scored_views - instock_views as oos_views
            , sum(case when current_price_type = 'R' then product_views end) AS product_views_reg
            , sum(case when current_price_type = 'R' then product_views*pct_instock end) AS instock_views_reg
            , sum(case when pct_instock is not null and current_price_type = 'R' then product_views end) AS scored_views_reg
        from t2dl_das_product_funnel.product_price_funnel_daily a
        inner join PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW c
          on a.event_date_pacific = c.day_date
        where event_date_pacific between (select min(day_date) from date_lkp) and (select max(day_date) from date_lkp)
          and channelcountry = 'US'
          and channel in ('FULL_LINE', 'RACK')
        group by 1,2,3,4,5
    )  a
    inner join sku_base d
      on a.rms_sku_num = d.rms_sku_num
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
) with data primary index (quarter_idnt, month_idnt, cc, channel) on commit preserve rows
;

-- Grab Live on Site
create multiset volatile table los as (
    select
        cc
        , month_idnt
        , channel
        , department
        , "class"
        , supplier
        , max(days_live) as days_live
    from (
        select
            sku_id as rms_sku_num
            , month_idnt
            , case when channel_brand = 'NORDSTROM' then '120, N.COM' else '250, OFFPRICE ONLINE' end as channel
            , count(distinct a.day_date) as days_live
        from t2dl_das_site_merch.live_on_site_daily a
        inner join PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW c
          on a.day_date = c.day_date
        where a.day_date between (select min(day_date) from date_lkp) and (select max(day_date) from date_lkp)
          and a.channel_country = 'US'
        group by 1,2,3
    ) a
    inner join sku_base d
      on a.rms_sku_num = d.rms_sku_num
    group by 1,2,3,4,5,6
) with data primary index (month_idnt, cc, channel, department, supplier) on commit preserve rows
;

--- Union KPIs
create multiset volatile table full_set as (
    select
        quarter_idnt
        , month_idnt
        , cc
        , channel
        , division
        , subdivision
        , department
        , "class"
        , subclass
        , brand_name
        , supplier
        , category
        , vpn
        , supplier_color
        , npg_flag
        , max(style_group_desc) as style_description
        , max(sg_desc) as sg_desc
        , max(style_group_num) as style_group_num
        , sum(coalesce(sales_r,0)) as sales_r
        , sum(coalesce(sales_u,0)) as sales_u
        , sum(coalesce(sales_c,0)) as sales_c
        , sum(coalesce(sales_r_reg,0)) as sales_r_reg
        , sum(coalesce(sales_r_pro,0)) as sales_r_pro
        , sum(coalesce(sales_r_clr,0)) as sales_r_clr
        , sum(coalesce(sales_u_reg,0)) as sales_u_reg
        , sum(coalesce(sales_u_pro,0)) as sales_u_pro
        , sum(coalesce(sales_u_clr,0)) as sales_u_clr
        , sum(coalesce(demand_r,0)) as demand_r
        , sum(coalesce(demand_u,0)) as demand_u
        , sum(coalesce(demand_r_reg,0)) as demand_r_reg
        , sum(coalesce(demand_r_pro,0)) as demand_r_pro
        , sum(coalesce(demand_r_clr,0)) as demand_r_clr
        , sum(coalesce(demand_u_reg,0)) as demand_u_reg
        , sum(coalesce(demand_u_pro,0)) as demand_u_pro
        , sum(coalesce(demand_u_clr,0)) as demand_u_clr
        , sum(coalesce(returns_r,0)) as returns_r
        , sum(coalesce(returns_u,0)) as returns_u
        , sum(eoh_u) as eoh_u
        , sum(eoh_r) as eoh_r
        , sum(eoh_c) as eoh_c
        , sum(eoh_u_regpro) as eoh_u_regpro
        , sum(eoh_u_clr) as eoh_u_clr

        , sum(receipts_r) as receipts_r
        , sum(receipts_u) as receipts_u
        , sum(receipts_c) as receipts_c
        , sum(dropship_receipts_r) as dropship_receipts_r
        , sum(dropship_receipts_u) as dropship_receipts_u

        , sum(product_views) AS product_views
        , sum(add_to_bag_quantity) AS add_to_bag_quantity
        , sum(order_quantity) AS order_quantity
        , sum(instock_views) AS instock_views
        , sum(scored_views) AS scored_views
        , sum(oos_views) as oos_views
        , sum(product_views_reg) as product_views_reg
        , sum(instock_views_reg) AS instock_views_reg
        , sum(scored_views_reg) AS scored_views_reg

        , sum(pah_r) as pah_r
        , sum(pah_u) as pah_u
        , sum(pah_c) as pah_c

        , max(price_band_aur) as price_band_aur
        , max(price_band_two_aur) as price_band_two_aur
--         , case when sum(receipts_u) = 0 or sum(receipts_u) is null then 0
--              else sum(dropship_receipts_u*1.00)/sum(receipts_u*1.00) end as pct_ds_receipts
--         , case when pct_ds_receipts >= .5 then 'Y' else 'N' end as ds_itent
--         , case when sum(receipts_u) > 10 then 'Y' else 'N' end as quarter_intent
--         , case when sum(receipts_u) > 0 then 'Y' else 'N' end as quarter_receipt_ind
--         , sum(receipts_u-dropship_receipts_u) as hard_book_receipts_u
--         , case when hard_book_receipts_u > 10 then 'Y' else 'N' end as quarter_hard_intent
--         , case when sum(demand_u) > 0 then 'Y' else 'N' end as demand_ind
        , case when sum(coalesce(sales_u,0)+eoh_u) = 0 then 0 else (sum(coalesce(sales_u,0))/sum(coalesce(sales_u,0)+eoh_u*1.000)) end as monthly_st
        , case when sum(coalesce(sales_u_reg,0)+coalesce(sales_u_pro,0)+eoh_u_regpro) = 0 then 0 else (sum(coalesce(sales_u_reg,0)+coalesce(sales_u_pro,0))/sum(coalesce(sales_u_reg,0)+coalesce(sales_u_pro,0)+eoh_u_regpro*1.00)) end as monthly_st_regpro
        , case when sum(coalesce(sales_u_clr,0)+eoh_u_clr) = 0 then 0 else (sum(coalesce(sales_u_clr,0))/sum(coalesce(sales_u_clr,0)+eoh_u_clr*1.00)) end as monthly_st_clr
    from (
    select * from sales_eoh_ncom
    union all
    select * from sales_eoh_rcom
    union all
    select * from rcpts
    union all
    select * from pfd
    union all
    select * from pah
    ) a
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
) with data primary index (quarter_idnt, month_idnt, cc, channel) on commit preserve rows
;

-- Create final table with deciles / other extra calcs
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'cc_dept_quintile_ncom', OUT_RETURN_MSG);
create multiset table {environment_schema}.cc_dept_quintile_ncom as (
    select
      a.*
      , pct_ds_receipts
      , ds_intent
      , quarter_intent
      , quarter_receipt_ind
      , hard_book_receipts_u
      , quarter_hard_intent
      , demand_ind
      , avg_monthly_st
      , avg_monthly_st_regpro
      , avg_monthly_st_clr
      , cml_pct
      , quintile
      , case when c.cc is not null then 'Y' else 'N' end as rp
      , new_cc
      , coalesce(d.days_live, 0) as days_live
    from full_set a
    left join (
        select
             a.*
            , case when sum(demand_u*1.000) over(partition by quarter_idnt, department, demand_ind order by demand_u desc) = 0 then 0
                else sum(demand_u) over(partition by quarter_idnt, department, demand_ind order by demand_u desc rows unbounded preceding)/
                  sum(demand_u*1.000) over(partition by quarter_idnt, department, demand_ind order by demand_u desc)
               end as cml_pct
            , case when demand_ind = 'N' then 6
                when cml_pct <= .2 then 1
                when cml_pct <= .4 then 2
                when cml_pct <= .6 then 3
                when cml_pct <= .8 then 4
                when cml_pct <= 1 then 5
              end as quintile
            , lag(receipts_u) over(partition by cc, department, "class", supplier order by quarter_idnt) as prior_q_receipts
            , case when lag(receipts_u) over(partition by cc, department, "class", supplier order by quarter_idnt) is null and receipts_u > 0 then 'Y'
                  when lag(receipts_u) over(partition by cc, department, "class", supplier order by quarter_idnt) = 0 and receipts_u > 0 then 'Y'
                  else 'N' end as new_cc
        from (
            select
                 quarter_idnt
                , cc
                , department
                , "class"
                , supplier
                , case when sum(demand_u) > 0 then 'Y' else 'N' end as demand_ind
                , sum(demand_u) as demand_u
                , sum(receipts_u) as receipts_u
                , sum(dropship_receipts_u) as dropship_receipts_u
                , case when sum(receipts_u) = 0 or sum(receipts_u) is null then 0
                     else sum(dropship_receipts_u*1.00)/sum(receipts_u*1.00) end as pct_ds_receipts
                , case when pct_ds_receipts >= .5 then 'Y' else 'N' end as ds_intent
                , case when sum(receipts_u) + sum(pah_u) > 10 then 'Y' else 'N' end as quarter_intent
                , case when sum(receipts_u) + sum(pah_u) > 0 then 'Y' else 'N' end as quarter_receipt_ind
                , sum(receipts_u-dropship_receipts_u) as hard_book_receipts_u
                , case when hard_book_receipts_u > 10 then 'Y' else 'N' end as quarter_hard_intent
                , avg(monthly_st) as avg_monthly_st
                , avg(monthly_st_regpro) as avg_monthly_st_regpro
                , avg(monthly_st_clr) as avg_monthly_st_clr
            from full_set
            where channel = '120, N.COM'
            group by 1,2,3,4,5
        ) a
    ) b
    on a.quarter_idnt = b.quarter_idnt
      and a.cc = b.cc
      and a.department = b.department
      and a."class" = b."class"
      and a.supplier = b.supplier
    left join rp_lkp c
    on a.month_idnt = c.month_idnt
      and a.cc = c.cc
      and a.department = c.department
      and a."class" = c."class"
      and a.supplier = c.supplier
   left join los d
     on a.month_idnt = d.month_idnt
       and a.cc = d.cc
       and a.department = d.department
       and a."class" = d."class"
       and a.supplier = d.supplier
       and d.channel = '120, N.COM'
   where quarter_intent is not null
        and a.channel = '120, N.COM'
        and a.quarter_idnt in (select distinct quarter_idnt from date_lkp)
) with data primary index (quarter_idnt, cc, department, supplier)
;

grant select on {environment_schema}.cc_dept_quintile_ncom to public
;

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'cc_dept_quintile_rcom', OUT_RETURN_MSG);
create multiset table {environment_schema}.cc_dept_quintile_rcom as (
    select
      a.*
      , pct_ds_receipts
      , ds_intent
      , quarter_intent
      , quarter_receipt_ind
      , hard_book_receipts_u
      , quarter_hard_intent
      , demand_ind
      , avg_monthly_st
      , avg_monthly_st_regpro
      , avg_monthly_st_clr
      , cml_pct
      , quintile
      , case when c.cc is not null then 'Y' else 'N' end as rp
      , new_cc
      , coalesce(d.days_live, 0) as days_live
    from full_set a
    left join (
        select
             a.*
            , case when sum(demand_u*1.000) over(partition by quarter_idnt, department, demand_ind order by demand_u desc) = 0 then 0
                else sum(demand_u) over(partition by quarter_idnt, department, demand_ind order by demand_u desc rows unbounded preceding)/
                  sum(demand_u*1.000) over(partition by quarter_idnt, department, demand_ind order by demand_u desc)
               end as cml_pct
            , case when demand_ind = 'N' then 6
                when cml_pct <= .2 then 1
                when cml_pct <= .4 then 2
                when cml_pct <= .6 then 3
                when cml_pct <= .8 then 4
                when cml_pct <= 1 then 5
              end as quintile
            , lag(receipts_u) over(partition by cc, department, "class", supplier order by quarter_idnt) as prior_q_receipts
            , case when lag(receipts_u) over(partition by cc, department, "class", supplier order by quarter_idnt) is null and receipts_u > 0 then 'Y'
                  when lag(receipts_u) over(partition by cc, department, "class", supplier order by quarter_idnt) = 0 and receipts_u > 0 then 'Y'
                  else 'N' end as new_cc
        from (
            select
                 quarter_idnt
                , cc
                , department
                , "class"
                , supplier
                , case when sum(demand_u) > 0 then 'Y' else 'N' end as demand_ind
                , sum(demand_u) as demand_u
                , sum(receipts_u) as receipts_u
                , sum(dropship_receipts_u) as dropship_receipts_u
                , case when sum(receipts_u) = 0 or sum(receipts_u) is null then 0
                     else sum(dropship_receipts_u*1.00)/sum(receipts_u*1.00) end as pct_ds_receipts
                , case when pct_ds_receipts >= .5 then 'Y' else 'N' end as ds_intent
                , case when sum(receipts_u) + sum(pah_u) > 10 then 'Y' else 'N' end as quarter_intent
                , case when sum(receipts_u) + sum(pah_u) > 0 then 'Y' else 'N' end as quarter_receipt_ind
                , sum(receipts_u-dropship_receipts_u) as hard_book_receipts_u
                , case when hard_book_receipts_u > 10 then 'Y' else 'N' end as quarter_hard_intent
                , avg(monthly_st) as avg_monthly_st
                , avg(monthly_st_regpro) as avg_monthly_st_regpro
                , avg(monthly_st_clr) as avg_monthly_st_clr
            from full_set
            where channel = '250, OFFPRICE ONLINE'
            group by 1,2,3,4,5
        ) a
    ) b
    on a.quarter_idnt = b.quarter_idnt
      and a.cc = b.cc
      and a.department = b.department
      and a."class" = b."class"
      and a.supplier = b.supplier
    left join rp_lkp c
    on a.month_idnt = c.month_idnt
      and a.cc = c.cc
      and a.department = c.department
      and a."class" = c."class"
      and a.supplier = c.supplier
   left join los d
     on a.month_idnt = d.month_idnt
       and a.cc = d.cc
       and a.department = d.department
       and a."class" = d."class"
       and a.supplier = d.supplier
       and d.channel = '250, OFFPRICE ONLINE'
   where quarter_intent is not null
        and a.channel = '250, OFFPRICE ONLINE'
        and a.quarter_idnt in (select distinct quarter_idnt from date_lkp)
) with data primary index (quarter_idnt, cc, department, supplier)
;

grant select on {environment_schema}.cc_dept_quintile_rcom  to public
;
