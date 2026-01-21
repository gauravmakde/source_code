SET QUERY_BAND = 'App_ID=APP08364;
     DAG_ID=twist_11521_ACE_ENG;
     Task_Name=twist_daily;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: twist_daily
Team/Owner: Meghan Hickey (meghan.d.hickey@nordstrom.com)
Date Created/Modified: 04/03/2023 
Updates:

  * modified eoh signal to asoh
  * bring in MC for N.COM NRP
  * add mc_instock_ind (default) and fc_instock_ind columns

Date Created/Modified: 04/15/2023 
Updates:

  * update MC logic to bring in store inv completely
  * update asoh/eoh also to reflect based on fc/mc

Note:
-- What is the the purpose of the table: migrating from NAPBI to ISF
-- What is the update cadence/lookback window: dependency pending to be the following data availability:
    't2dl_das_product_funnel.product_funnel_daily',
    't2dl_das_fls_traffic_model.store_traffic_daily_vw',
    'prd_nap_usr_vws.retail_tran_detail_fact_vw',
    'prd_nap_usr_vws.inventory_stock_quantity_by_day_fact'

*/


/* Step 1: Product Views by Style/Channel */
create multiset volatile table product_views
  as (
    select
        event_date_pacific
        , case when channel = 'FULL_LINE' and channelcountry = 'US' then 'N.COM'
            when channel = 'FULL_LINE' and channelcountry = 'CA' then 'N.CA'
            when channel = 'RACK' and channelcountry = 'US' then 'OFFPRICE ONLINE'
            end as business_unit_desc
        , channelcountry
        , style_id
        , sum(product_views) as product_views
    from t2dl_das_product_funnel.product_funnel_daily
    where event_date_pacific between '2024-08-25' and '2024-08-25' and site_source = 'MODERN'
    group by 1, 2, 3, 4


    union all

    select
        event_date_pacific
        , case when channel = 'FULL_LINE' and channelcountry = 'US' then 'N.COM'
            when channel = 'FULL_LINE' and channelcountry = 'CA' then 'N.CA'
            when channel = 'RACK' and channelcountry = 'US' then 'OFFPRICE ONLINE'
            end as business_unit_desc
        , channelcountry
        , web_style_id as style_id
        , sum(product_views) as product_views
    from t2dl_das_product_funnel.product_funnel_daily_history
    where event_date_pacific between '2024-08-25' and '2024-08-25'
    group by 1, 2, 3, 4

) with data primary index (style_id, event_date_pacific, business_unit_desc, channelcountry) on commit preserve rows;

collect statistics primary index (style_id, event_date_pacific, business_unit_desc, channelcountry)
  , column(style_id)
  , column(event_date_pacific)
  , column(business_unit_desc)
  , column(channelcountry)
on product_views;


/* Step 2: Store Traffic by Location/Day */

create multiset volatile table pre_store_traffic as (
select stdn.store_num as store_number
, stdn.day_date
, week_num
,channel as business_unit_desc,
sum(purchase_trips) as purchase_trips,
sum(case when traffic_source in ('retailnext_cam') then traffic else 0 end) as traffic_rn_cam,
sum(case when traffic_source in ('placer_channel_scaled_to_retailnext', 'placer_closed_store_scaled_to_retailnext','placer_vertical_interference_scaled_to_retailnext', 'placer_vertical_interference_scaled_to_est_retailnext','placer_store_scaled_to_retailnext') then traffic else 0 end) as traffic_scaled_placer
from t2dl_das_fls_traffic_model.store_traffic_daily as stdn
inner join PRD_NAP_USR_VWS.DAY_CAL as dc on stdn.day_date = dc.day_date
where 1=1
and year_num >= 2021
and stdn.day_date between '2024-08-25' and '2024-08-25'
group by 1,2,3,4
)  with data primary index (store_number, day_date) on commit preserve rows;

collect statistics primary index (store_number, day_date)
  , column(store_number)
  , column(day_date)
on pre_store_traffic
;

create multiset volatile table store_traffic
  as (
    select
        day_date
        , t.business_unit_desc
        , store_number
        , sum(case when traffic_rn_cam > 0 then traffic_rn_cam else traffic_scaled_placer end) as traffic
    from pre_store_traffic t
    left join prd_nap_usr_vws.store_dim s on s.store_num = t.store_number
    where day_date between '2024-08-25' and '2024-08-25' and s.store_type_code in ('FL', 'RK')
    group by 1, 2, 3
) with data primary index (store_number, day_date) on commit preserve rows;


collect statistics primary index (store_number, day_date)
  , column(store_number)
  , column(day_date)
on store_traffic
;

/* 3. Demand by Sku/Channel*/
create multiset volatile table demand
  as (
    select
        business_day_date
        , web_style_num
        , rms_style_num
        , trn.rms_sku_num
        , case when business_unit_desc = 'N.COM' then '808'
            when business_unit_desc = 'OFFPRICE ONLINE' then '828'
            else cast(intent_store_num as varchar(5))
          end as store_num
        , business_unit_desc
        , sum(line_net_usd_amt) as demand
        , sum(line_item_quantity) as items
    from prd_nap_usr_vws.retail_tran_detail_fact trn
    inner join prd_nap_usr_vws.store_dim st
      on trn.intent_store_num = st.store_num
    inner join prd_nap_usr_vws.product_sku_dim_vw sku
      on coalesce(trn.sku_num, trn.hl_sku_num) = sku.rms_sku_num
      and case when st.business_unit_desc like '%CANADA%' then 'CA' else 'US' end = sku.channel_country
    where business_day_date between cast('2024-08-25' as date) - 60 and cast('2024-08-25' as date)
       and line_net_usd_amt > 0
       and line_item_quantity > 0
       and business_unit_desc in ('N.COM', 'FULL LINE', 'RACK', 'RACK CANADA', 'OFFPRICE ONLINE')
	     and tran_type_code <> 'PAID'
    group by 1, 2, 3, 4, 5, 6
) with data primary index (rms_sku_num, rms_style_num, store_num, business_day_date) on commit preserve rows;

collect statistics primary index (rms_sku_num, rms_style_num, store_num, business_day_date)
  , column(rms_sku_num)
  , column(rms_style_num)
  , column(store_num)
  , column(business_day_date)
on demand;


/* 4. Pick up RP Lookup Flag */
create multiset volatile table rp_lkp
  as (
      select
          rms_sku_num
          , cast(st.store_num as varchar(5)) as loc_idnt
          , day_date
          , business_unit_desc
          , 'Y' as aip_rp_fl
      from PRD_NAP_USR_VWS.MERCH_RP_SKU_LOC_DIM_HIST rp
      inner join (
                select distinct day_date
                from prd_nap_usr_vws.day_cal
                where day_date between cast('2024-08-25' as date) - 60 and cast('2024-08-25' as date)
            ) d
      on d.day_date between BEGIN(rp_period) and END(rp_period)
      inner join (
        select
            str.*
            , case when store_num in (210, 212) then 209 else store_num end as store_num_stg
        from prd_nap_usr_vws.store_dim str
        where business_unit_desc IN ('N.COM', 'FULL LINE',  'RACK', 'RACK CANADA', 'OFFPRICE ONLINE', 'N.CA')
      ) st
         on rp.location_num = st.store_num_stg
      group by 1,2,3,4,5
) with data primary index (rms_sku_num, loc_idnt, day_date) on commit preserve rows;

collect statistics primary index (rms_sku_num, loc_idnt, day_date)
  , column(rms_sku_num)
  , column(loc_idnt)
  , column(day_date)
on rp_lkp;


/* 5. BOH by Sku/Day/Loc */
create multiset volatile table eoh_base
  as (
      select
          snapshot_date as day_date
          , case when location_type  in ('DS', 'DS_OP', 'ECONCESSION') then location_type else business_unit_desc end as bu
          , case when bu in ('N.COM', 'OMNI.COM', 'DS', 'ECONCESSION') then '808'
              when bu in ('N.CA', 'FULL LINE CANADA') then '867'
              when bu in ('OFFPRICE ONLINE', 'DS_OP') then '828'
              else location_id end as store_num
          , case when bu in ('N.COM', 'N.CA', 'FULL LINE', 'FULL LINE CANADA', 'OMNI.COM', 'DS', 'ECONCESSION') then 'NORD' else 'RACK' end as banner
          , case when bu = 'DS' then 'N.COM'
              when bu = 'ECONCESSION' then 'N.COM'
              when bu = 'DS_OP' then 'OFFPRICE ONLINE'
              when bu = 'OMNI.COM' then 'N.COM'
              else bu end as business_unit_desc
          , 'US' as country
          , rms_sku_id as rms_sku_num
          , stock_on_hand_qty
          , immediately_sellable_qty
      from PRD_NAP_USR_VWS.INVENTORY_STOCK_QUANTITY_BY_DAY_PHYSICAL_FACT base
      left join prd_nap_usr_vws.store_dim st
        on base.location_id = st.store_num
      where snapshot_date between cast('2024-08-25' as date) - 61 and cast('2024-08-25' as date) - 1 -- go one day back for BOH casting
          and stock_on_hand_qty > 0
          and bu in ('N.COM', 'FULL LINE',  'RACK', 'OFFPRICE ONLINE', 'OMNI.COM', 'DS', 'DS_OP', 'ECONCESSION')
) with data primary index (day_date, store_num, rms_sku_num) on commit preserve rows;

collect statistics primary index (day_date, store_num, rms_sku_num)
  , column(rms_sku_num)
  , column(store_num)
  , column(day_date)
on eoh_base;

/* 6. Bring in FLS eoh, asoh to add to Ncom eoh and asoh only for MC */

create multiset volatile table eoh_stg_mc AS (
  select
          DISTINCT inv.day_date
          , inv.bu
          , inv.store_num
          , inv.banner
          , inv.business_unit_desc
          , inv.country
          , inv.rms_sku_num
          , inv.stock_on_hand_qty
          , inv.immediately_sellable_qty
          , fls.stock_on_hand_qty AS fls_eoh
          , fls.immediately_sellable_qty AS fls_asoh
          , coalesce(inv.stock_on_hand_qty,0) + coalesce(fls_eoh,0) AS mc_stock_on_hand_qty
          , coalesce(inv.immediately_sellable_qty,0) + coalesce(fls_asoh,0) AS mc_immediately_sellable_qty       
      from eoh_base inv 
       left JOIN 
      (
      SELECT DISTINCT day_date, rms_sku_num,sum(stock_on_hand_qty) AS stock_on_hand_qty , sum(immediately_sellable_qty) AS immediately_sellable_qty
      FROM eoh_base fls WHERE business_unit_desc = 'FULL LINE' 
      group BY 1,2) fls  
      ON fls.rms_sku_num = inv.rms_sku_num AND inv.day_date = fls.day_date 
      where inv.business_unit_desc = 'N.COM' 
 ) with data primary index (day_date, store_num, rms_sku_num, business_unit_desc) on commit preserve rows;

collect statistics primary index (day_date, store_num, rms_sku_num, business_unit_desc)
  , column(rms_sku_num)
  , column(store_num)
  , column(day_date)
  , column(business_unit_desc)
on eoh_stg_mc;

CREATE multiset volatile table eoh_stg_tmp AS 
(
SELECT * FROM eoh_stg_mc
UNION ALL 
SELECT eoh_base.*,  0 AS fls_eoh, 0 AS fls_asoh, 0 AS mc_stock_on_hand_qty, 0 AS mc_immediately_sellable_qty FROM eoh_base 
WHERE business_unit_desc NOT IN ('N.COM')
) with data primary index (day_date, store_num, rms_sku_num, business_unit_desc) on commit preserve rows;

collect statistics primary index (day_date, store_num, rms_sku_num, business_unit_desc)
  , column(rms_sku_num)
  , column(store_num)
  , column(day_date)
  , column(business_unit_desc)
on eoh_stg_tmp;

create multiset volatile table eoh_stg
  as (
      select
          inv.day_date + 1 as day_date -- base TWIST on BOH not EOH so cast EOH to beigning of next day
          , inv.day_date as lag_day
          , case when day_date <= cast('2022-04-01' as date) then cast(coalesce(pcdb.web_style_id, sku.web_style_num) as BIGINT)
                else cast(sku.web_style_num as BIGINT) end as web_style_num
          , coalesce(sku.style_group_num, pcdb.rms_style_group_num) as rms_style_group_num
          , sku.rms_style_num
          , inv.rms_sku_num
          , store_num
          , banner
          , business_unit_desc
          , country
          , sum(stock_on_hand_qty) as eoh
          , sum(immediately_sellable_qty) as asoh
          , sum(mc_stock_on_hand_qty) as eoh_mc
          , sum(mc_immediately_sellable_qty) as asoh_mc
      from eoh_stg_tmp inv
      inner join prd_nap_usr_vws.product_sku_dim_vw sku
        on inv.rms_sku_num = sku.rms_sku_num and sku.channel_country = 'US'
      left join T2DL_DAS_ETE_INSTRUMENTATION.LIVE_ON_SITE_PCDB_US_OPR pcdb
        on inv.rms_sku_num = pcdb.rms_sku_id
      where coalesce(web_style_num, rms_style_num) is not null
      group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
) with data primary index (rms_sku_num, web_style_num, rms_style_num, day_date, store_num, business_unit_desc) on commit preserve rows ;

collect statistics primary index (rms_sku_num, web_style_num, rms_style_num, day_date, store_num, business_unit_desc)
  , column(rms_sku_num)
  , column(web_style_num)
  , column(rms_style_num)
  , column(day_date)
  , column(store_num)
  , column(business_unit_desc)
on eoh_stg;


/* 6. Create Daily Availability Lookbacks */
create multiset volatile table twist_blowout
  as (
    select
      base.web_style_num
      , base.rms_style_group_num
      , base.rms_style_num
      , base.rms_sku_num
      , base.store_num
      , base. business_unit_desc
      , base.country
      , base.banner
      , base.min_date
      , base.max_date
      , base.max_units
      , base.max_style
      , xday.day_date
    from (
        select
          web_style_num
          , rms_style_group_num
          , rms_style_num
          , rms_sku_num
          , store_num
          , business_unit_desc
          , country
          , banner
          , min(day_date) as min_date
          , max(day_date) as max_date
          , max(eoh) as max_units
          , sum(max(asoh)) over(partition by banner, business_unit_desc, store_num, coalesce(rms_style_group_num, web_style_num, rms_style_num)) as max_style -- change to ASOH
        from eoh_stg
        group by 1, 2, 3, 4, 5, 6, 7, 8
    ) base
    cross join (
        select
            day_date
        from prd_nap_usr_vws.day_cal
        where day_date between cast('2024-08-25' as date) and cast('2024-08-25' as date)
    ) xday
    where day_date >= min_date
      and day_date - max_date <= 60
      and max_style > 1
) with data primary index (rms_sku_num, web_style_num, rms_style_num, day_date, store_num, business_unit_desc) on commit preserve rows ;

collect statistics primary index (rms_sku_num, web_style_num, rms_style_num, day_date, store_num, business_unit_desc)
  , column(rms_sku_num)
  , column(web_style_num)
  , column(rms_style_num)
  , column(day_date)
  , column(store_num)
  , column(business_unit_desc)
on twist_blowout;

/*7. Bring in FC items*/

create multiset volatile table combine
  as (
    select
        base.web_style_num
        , base.rms_style_group_num
        , base.rms_style_num
        , base.rms_sku_num
        , base.store_num
        , base.business_unit_desc
        , base.country
        , base.banner
        , base.min_date
        , base.max_date
        , base.max_units
        , base.max_style
        , base.day_date
        , coalesce(dm.dma_code, 0) as dma
        , coalesce(eoh.eoh,0) as eoh
        , coalesce(eoh.asoh,0) as asoh
        , coalesce(eoh.eoh_mc,0) as eoh_mc
        , coalesce(eoh.asoh_mc,0) as asoh_mc
        --, sum(coalesce(eoh.asoh,0)) over(partition by base.web_style_num, base.rms_sku_num, base.business_unit_desc, base.day_date, dma) as dma_eoh
        , case when rp.aip_rp_fl = 'Y' then 1 else 0 end as rp_idnt
        /* in stock indicator based on business thresholds until availability data is ready */
        , case when base.business_unit_desc in ('RACK', 'RACK CANADA', 'FULL LINE') and eoh.asoh > 0 then 1
             when base.business_unit_desc in ('N.CA', 'OFFPRICE ONLINE') and eoh.asoh > 1 then 1
             when base.business_unit_desc in ('N.COM') and eoh.asoh_mc > 1 then 1
          else 0 end as mc_instock_ind
        , case when base.business_unit_desc in ('RACK', 'RACK CANADA', 'FULL LINE') and eoh.asoh > 0 then 1
               when base.business_unit_desc in ('N.COM', 'N.CA', 'OFFPRICE ONLINE') and eoh.asoh > 1 then 1
          else 0 end as fc_instock_ind
        , coalesce(dmd.demand,0) as demand
        , coalesce(dmd.items,0) as items
        , coalesce(pv.product_views,0) as product_views
        , coalesce(st.traffic,0) as traffic
        , price_store_num
    from twist_blowout base
    left join eoh_stg eoh
    on base.day_date = eoh.day_date
        and base.rms_sku_num = eoh.rms_sku_num
        and base.store_num = eoh.store_num
    left join rp_lkp rp
    on base.rms_sku_num = rp.rms_sku_num
        and base.store_num = rp.loc_idnt
        and base.day_date = rp.day_date
    left join demand dmd
    on base.day_date = dmd.business_day_date
        and base.rms_sku_num = dmd.rms_sku_num
        and base.store_num = cast(dmd.store_num as int)
    left join product_views pv
    on base.day_date = pv.event_date_pacific
        and base.web_style_num = pv.style_id
        and base.country = pv.channelcountry
        and base.banner = case when pv.business_unit_desc in ('N.COM', 'N.CA', 'FULL LINE') then 'NORD' else 'RACK' end
    left join store_traffic st
    on base.day_date = st.day_date
        and base.store_num = st.store_number
        and base.business_unit_desc = st.business_unit_desc
    left join (
        select distinct store_num, us_dma_code as dma_code from prd_nap_usr_vws.org_store_us_dma
        union all
        select distinct store_num, ca_dma_code as dma_code from prd_nap_usr_vws.org_store_ca_dma
    ) dm
    on base.store_num = dm.store_num
    left join prd_nap_usr_vws.price_store_dim_vw sr
    on base.store_num = sr.store_num
) with data primary index (rms_sku_num, web_style_num, rms_style_num, day_date, store_num, business_unit_desc, dma, price_store_num) on commit preserve rows ;

collect statistics primary index (rms_sku_num, web_style_num, rms_style_num, day_date, store_num, business_unit_desc, dma, price_store_num)
  , column(rms_sku_num)
  , column(web_style_num)
  , column(rms_style_num)
  , column(day_date)
  , column(store_num)
  , column(business_unit_desc)
  , column(dma)
  , column(price_store_num)
on combine;

create multiset volatile table combine_prc
  as (
    select
        base.*
        , case when ownership_retail_price_type_code = 'CLEARANCE' then 'C'
              when ownership_retail_price_type_code = 'REGULAR' then 'R'
              else null
          end as ownership_price_type
        , case when selling_retail_price_type_code = 'CLEARANCE' or ownership_retail_price_type_code = 'CLEARANCE' then 'C'
              when selling_retail_price_type_code = 'REGULAR' then 'R'
              when selling_retail_price_type_code = 'PROMOTION' then 'P'
              else null
          end as current_price_type
        , selling_retail_price_amt as current_price_amt
    from combine base
    left join prd_nap_usr_vws.PRODUCT_PRICE_TIMELINE_DIM pr
    on base.rms_sku_num = pr.rms_sku_num
        and base.price_store_num = pr.store_num
				and cast(base.day_date as timestamp) between pr.EFF_BEGIN_TMSTP AND pr.EFF_END_TMSTP - interval '0.001' second
) with data primary index (rms_sku_num, web_style_num, rms_style_num, day_date, store_num, business_unit_desc, dma) on commit preserve rows ;

collect statistics primary index (rms_sku_num, web_style_num, rms_style_num, day_date, store_num, business_unit_desc, dma)
  , column(rms_sku_num)
  , column(web_style_num)
  , column(rms_style_num)
  , column(day_date)
  , column(store_num)
  , column(business_unit_desc)
  , column(dma)
on combine_prc;

/* 9. Grab department product view totals to partition by*/
create multiset volatile table combine_pvs
  as (
    select
        c.web_style_num
        , c.rms_style_group_num
        , c.rms_style_num
        , c.rms_sku_num
        , c.store_num
        , c.business_unit_desc
        , c.country
        , c.banner
        , c.min_date
        , c.max_date
        , c.max_units
        , c.max_style
        , c.day_date
        , c.dma
        , c.eoh_mc
        , c.eoh
        , c.asoh_mc        
        , c.asoh
        --, c.dma_eoh
        , c.rp_idnt
        , c.mc_instock_ind
        , c.fc_instock_ind
        , c.demand
        , c.items
        , c.product_views
        , c.traffic
        , d.dept_num
        , d.dept_pvs
        , c.ownership_price_type
        , c.current_price_type
        , c.current_price_amt
    from combine_prc c
    left join (
        select
            stg.day_date
            , stg.web_style_num
            , stg.dept_num
            , stg.product_views
            , stg.store_num
            , sum(product_views) over(partition by day_date, dept_num, store_num) as dept_pvs
        from (
            select
                distinct c.day_date
                , c.web_style_num
                , s.dept_num
                , c.product_views
                , c.store_num
            from combine c
            inner join prd_nap_usr_vws.product_sku_dim_vw s
              on c.rms_sku_num = s.rms_sku_num
              and c.country = s.channel_country
            where business_unit_desc in ('N.COM', 'FULL LINE') and c.web_style_num is not null
        ) stg
    ) d
    on c.day_date = d.day_date
      and c.web_style_num = d.web_style_num
      and c.store_num = d.store_num
    where business_unit_desc in ('N.COM', 'FULL LINE')
) with data primary index (rms_sku_num, web_style_num, day_date, store_num, business_unit_desc, dma) on commit preserve rows ;

collect statistics primary index (rms_sku_num, web_style_num, day_date, store_num, business_unit_desc, dma)
  , column(rms_sku_num)
  , column(web_style_num)
  , column(day_date)
  , column(store_num)
  , column(business_unit_desc)
  , column(dma)
on combine_pvs;


/* 9. Split out each channel with their own allocation logic starting with FLS */
create multiset volatile table combine_fls
  as (
    select
        cmb.web_style_num
        , cmb.rms_style_group_num
        , cmb.rms_style_num
        , cmb.rms_sku_num
        , cmb.store_num
        , cmb.business_unit_desc
        , cmb.country
        , cmb.banner
        , cmb.min_date
        , cmb.max_date
        , cmb.max_units
        , cmb.max_style
        , cmb.day_date
        , cmb.dma
        , cmb.eoh_mc
        , cmb.eoh
        , cmb.asoh_mc        
        , cmb.asoh
        --, cmb.dma_eoh
        , cmb.rp_idnt
        , cmb.ownership_price_type
        , cmb.current_price_type
        , cmb.current_price_amt
        , cmb.mc_instock_ind
        , cmb.fc_instock_ind
        , cmb.demand
        , cmb.items
        , cmb.product_views
        , cmb.traffic
        , cmb.dept_num
        , cmb.dept_pvs
        , coalesce(sum(dmd.items),0) as hist_items
        , coalesce(sum(hist_items) over(partition by cmb.day_date, cmb.business_unit_desc, cmb.store_num, cmb.web_style_num),0) as hist_items_style
        , case when hist_items_style = 0 then 0 else hist_items*1.000 /hist_items_style*1.000 end as pct_items
        , coalesce(sum(hist_items) over(partition by cmb.day_date, cmb.business_unit_desc, cmb.store_num, cmb.dept_num),0) as hist_items_dept
        , coalesce(sum(hist_items) over(partition by cmb.day_date, cmb.business_unit_desc, cmb.store_num),0) as hist_items_loc
        , coalesce(sum(1) over(partition by cmb.day_date, cmb.business_unit_desc, cmb.store_num, cmb.web_style_num),0) as rn
        , case when hist_items_loc = 0 then 0 else hist_items_dept*1.0000000/hist_items_loc end as pct_dept
        , traffic * pct_dept as dept_traffic
        , product_views*1.000000/dept_pvs as dept_pvs_pct
        , dept_pvs_pct * dept_traffic as style_traffic
    from combine_pvs cmb
    left join demand dmd
    on dmd.business_day_date between cmb.day_date - 60 and cmb.day_date - 1
      and cmb.web_style_num = dmd.web_style_num
      and cmb.rms_style_num = dmd.rms_style_num
      and cmb.rms_sku_num = dmd.rms_sku_num
      and cmb.business_unit_desc = dmd.business_unit_desc
      and cmb.store_num = dmd.store_num
    where cmb.business_unit_desc = 'FULL LINE' and dept_pvs > 0
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30
) with data primary index (rms_sku_num, web_style_num, day_date, store_num, business_unit_desc, dma) on commit preserve rows ;

collect statistics primary index (rms_sku_num, web_style_num, day_date, store_num, business_unit_desc, dma)
  , column(rms_sku_num)
  , column(web_style_num)
  , column(day_date)
  , column(store_num)
  , column(business_unit_desc)
  , column(dma)
on combine_fls;


create multiset volatile table fls_full
  as (
    select
        cf.web_style_num
        , cf.rms_style_group_num
        , cf.rms_style_num
        , cf.rms_sku_num
        , cf.store_num
        , cf.business_unit_desc
        , cf.country
        , cf.banner
        , cf.min_date
        , cf.max_date
        , cf.max_units
        , cf.max_style
        , cf.day_date
        , cf.dma
        , cf.eoh_mc
        , cf.eoh
        , cf.asoh_mc        
        , cf.asoh
        --, cf.dma_eoh
        , cf.rp_idnt
        , cf.ownership_price_type
        , cf.current_price_type
        , cf.current_price_amt
        , cf.mc_instock_ind
        , cf.fc_instock_ind
        , cf.demand
        , cf.items
        , cf.product_views
        , cf.traffic
        , cf.dept_num
        , cf.dept_pvs
        , cf.hist_items
        , cf.hist_items_style
        , cf.pct_items
        , cf.hist_items_dept
        , cf.hist_items_loc
        , cf.rn
        , cf.pct_dept
        , cf.dept_traffic
        , cf.dept_pvs_pct
        , cf.style_traffic
        , zero.style_total
        , zero.zero_ind
        , case when hist_items_style = 0 and rn = 1 then 1.000 * style_traffic --no demand for style group and only one size availble
             when hist_items_style = 0 and zero_ind = 1 then 1.000/rn * style_traffic -- no demand for style group across all sizes, more than one
             when hist_items_style = 0 then 0.000 -- no demand for just one size within style, set to 0
             else style_traffic * pct_items *1.000
        end as allocated_traffic
      from combine_fls cf
      left join (
          select
              day_date,
              business_unit_desc,
              web_style_num,
              store_num,
              sum(pct_items) as style_total,
              case when style_total = 0 then 1 else 0 end as zero_ind
          from combine_fls
          group by 1,2,3,4
     ) zero
     on cf.day_date = zero.day_date
       and cf.store_num = zero.store_num
       and cf.web_style_num = zero.web_style_num
) with data primary index (rms_sku_num, web_style_num, day_date, store_num, business_unit_desc, dma) on commit preserve rows ;

collect statistics primary index (rms_sku_num, web_style_num, day_date, store_num, business_unit_desc, dma)
  , column(rms_sku_num)
  , column(web_style_num)
  , column(day_date)
  , column(store_num)
  , column(business_unit_desc)
  , column(dma)
on fls_full;


/* 10. Repeat for Rack Logic Without Product View Look */
create multiset volatile table combine_rack
  as (
    select
        cmb.web_style_num
        , cmb.rms_style_group_num
        , cmb.rms_style_num
        , cmb.rms_sku_num
        , cmb.store_num
        , cmb.business_unit_desc
        , cmb.country
        , cmb.banner
        , cmb.min_date
        , cmb.max_date
        , cmb.max_units
        , cmb.max_style
        , cmb.day_date
        , cmb.dma
        , cmb.eoh_mc
        , cmb.eoh
        , cmb.asoh_mc        
        , cmb.asoh
        --, cmb.dma_eoh
        , cmb.rp_idnt
        , cmb.ownership_price_type
        , cmb.current_price_type
        , cmb.current_price_amt
        , cmb.mc_instock_ind
        , cmb.fc_instock_ind
        , cmb.demand
        , cmb.items
        , cmb.product_views
        , cmb.traffic
        , coalesce(sum(dmd.items),0) as hist_items
        , coalesce(sum(hist_items) over(partition by cmb.day_date, cmb.business_unit_desc, cmb.store_num, cmb.rms_style_num),0) as hist_items_style
        , case when hist_items_style = 0 then 0 else hist_items*1.000 /hist_items_style*1.000 end as pct_items
        , coalesce(sum(hist_items) over(partition by cmb.day_date, cmb.business_unit_desc, cmb.store_num),0) as hist_items_loc
        , coalesce(sum(1) over(partition by cmb.day_date, cmb.business_unit_desc, cmb.store_num, cmb.rms_style_num),0) as rn
        , case when hist_items_loc = 0 then 0 else hist_items_style*1.0000000/hist_items_loc end as pct_style
        , traffic * pct_style as style_traffic
        , style_traffic * pct_items as allocated_traffic
    from combine_prc cmb
    left join demand dmd
    on dmd.business_day_date between cmb.day_date - 60 and cmb.day_date - 1
      and cmb.rms_style_num = dmd.rms_style_num
      and cmb.rms_sku_num = dmd.rms_sku_num
      and cmb.business_unit_desc = dmd.business_unit_desc
      and cmb.store_num = dmd.store_num
    where cmb.business_unit_desc in ('RACK', 'RACK CANADA')
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
) with data primary index (rms_sku_num, web_style_num,  rms_style_num, day_date, store_num, business_unit_desc, dma) on commit preserve rows ;

collect statistics primary index (rms_sku_num, web_style_num, rms_style_num, day_date, store_num, business_unit_desc, dma)
  , column(rms_sku_num)
  , column(web_style_num)
  , column(rms_style_num)
  , column(day_date)
  , column(store_num)
  , column(business_unit_desc)
  , column(dma)
on combine_rack;


/* 11. Prep & Apply logic for N.com/N.ca/R.com */ 
create multiset volatile table combine_digital
  as (
    select
        cmb.web_style_num
        , cmb.rms_style_group_num
        , cmb.rms_style_num
        , cmb.rms_sku_num
        , cmb.store_num
        , cmb.business_unit_desc
        , cmb.country
        , cmb.banner
        , cmb.min_date
        , cmb.max_date
        , cmb.max_units
        , cmb.max_style
        , cmb.day_date
        , cmb.dma
        , cmb.eoh_mc
        , cmb.eoh
        , cmb.asoh_mc        
        , cmb.asoh  --, cmb.dma_eoh
        , cmb.rp_idnt
        , cmb.ownership_price_type
        , cmb.current_price_type
        , cmb.current_price_amt
        , cmb.mc_instock_ind
        , cmb.fc_instock_ind
        , cmb.demand
        , cmb.items
        , cmb.product_views
        , cmb.traffic
        , coalesce(sum(dmd.items),0) as hist_items
        , coalesce(sum(hist_items) over(partition by cmb.business_unit_desc, cmb.country, cmb.day_date, cmb.web_style_num),0) as hist_items_style
        , coalesce(sum(1) over(partition by cmb.business_unit_desc, cmb.day_date, cmb.store_num, cmb.web_style_num),0) as rn
        , case when hist_items_style = 0 then 0 else hist_items*1.00000/hist_items_style end as pct_items
    from combine_prc cmb
    left join demand dmd
    on dmd.business_day_date between cmb.day_date - 60 and cmb.day_date - 1
      and cmb.web_style_num = dmd.web_style_num
      and cmb.rms_style_num = dmd.rms_style_num
      and cmb.rms_sku_num = dmd.rms_sku_num
      and cmb.business_unit_desc = dmd.business_unit_desc
      and cmb.store_num = dmd.store_num
    where cmb.business_unit_desc in ('N.COM', 'N.CA', 'OFFPRICE ONLINE') and cmb.web_style_num is not null
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
) with data primary index (rms_sku_num, web_style_num, day_date, store_num, business_unit_desc, dma) on commit preserve rows ;

collect statistics primary index (rms_sku_num, web_style_num, day_date, store_num, business_unit_desc, dma)
  , column(rms_sku_num)
  , column(web_style_num)
  , column(day_date)
  , column(store_num)
  , column(business_unit_desc)
  , column(dma)
on combine_digital;


create multiset volatile table digital_full
  as (
      select
        cdg.web_style_num
        , cdg.rms_style_group_num
        , cdg.rms_style_num
        , cdg.rms_sku_num
        , cdg.store_num
        , cdg.business_unit_desc
        , cdg.country
        , cdg.banner
        , cdg.min_date
        , cdg.max_date
        , cdg.max_units
        , cdg.max_style
        , cdg.day_date
        , cdg.dma
        , cdg.eoh_mc
        , cdg.eoh
        , cdg.asoh_mc        
        , cdg.asoh   --, cdg.dma_eoh
        , cdg.rp_idnt
        , cdg.ownership_price_type
        , cdg.current_price_type
        , cdg.current_price_amt
        , cdg.mc_instock_ind
        , cdg.fc_instock_ind
        , cdg.demand
        , cdg.items
        , cdg.product_views
        , cdg.traffic
        , cdg.hist_items
        , cdg.hist_items_style
        , cdg.rn
        , cdg.pct_items
        , zero.style_total
        , zero.zero_ind
        , case when hist_items_style = 0 and rn = 1 then 1.00000 * product_views --no demand for style group and only one size availble
             when hist_items_style = 0 and zero_ind = 1 then 1.00000/rn * product_views -- no demand for style group across all sizes, more than one
             when hist_items_style = 0 then 0.00000 -- no demand for just one size within style, set to 0
             else product_views * pct_items * 1.00000
        end as allocated_traffic
      from combine_digital cdg
      left join (
          select
              day_date,
              business_unit_desc,
              web_style_num,
              store_num,
              sum(pct_items) as style_total,
              case when style_total = 0 then 1 else 0 end as zero_ind
          from combine_digital
          group by 1,2,3,4
     ) zero
     on cdg.day_date = zero.day_date
       and cdg.store_num = zero.store_num
       and cdg.web_style_num = zero.web_style_num
) with data primary index (rms_sku_num, web_style_num, day_date, store_num, business_unit_desc, dma) on commit preserve rows ;

collect statistics primary index (rms_sku_num, web_style_num, day_date, store_num, business_unit_desc, dma)
  , column(rms_sku_num)
  , column(web_style_num)
  , column(day_date)
  , column(store_num)
  , column(business_unit_desc)
  , column(dma)
on digital_full;

delete from T2DL_DAS_TWIST.twist_daily where day_date between cast('2024-08-25' as date) and cast('2024-08-25' as date);

insert into T2DL_DAS_TWIST.twist_daily
  select distinct
    day_date
    , country
    , banner
    , business_unit_desc
    , dma
    , store_num
    , web_style_num
    , rms_style_num
    , rms_sku_num
    , rp_idnt
    , ownership_price_type
    , current_price_type
    , current_price_amt
    , eoh_mc
    , eoh
    , asoh_mc        
    , asoh  
    , mc_instock_ind
    , fc_instock_ind
    , demand
    , items
    , product_views
    , traffic
    , hist_items
    , hist_items_style
    , pct_items
    , allocated_traffic
    , CURRENT_TIMESTAMP as dw_sys_load_tmstp
  from fls_full

  union all

  select distinct
    day_date
    , country
    , banner
    , business_unit_desc
    , dma
    , store_num
    , web_style_num
    , rms_style_num
    , rms_sku_num
    , rp_idnt
    , ownership_price_type
    , current_price_type
    , current_price_amt
    , eoh_mc
    , eoh
    , asoh_mc        
    , asoh
    , mc_instock_ind
    , fc_instock_ind
    , demand
    , items
    , product_views
    , traffic
    , hist_items
    , hist_items_style
    , pct_items
    , allocated_traffic
    , CURRENT_TIMESTAMP as dw_sys_load_tmstp
  from digital_full
  where business_unit_desc = 'N.COM'
    -- or (business_unit_desc = 'N.CA' and day_date >= '2021-01-30') --Only Since FY2020
    or (business_unit_desc = 'OFFPRICE ONLINE' and day_date >= '2021-01-31') -- Only Since FY2021

  union all

  select distinct
    day_date
    , country
    , banner
    , business_unit_desc
    , dma
    , store_num
    , web_style_num
    , rms_style_num
    , rms_sku_num
    , rp_idnt
    , ownership_price_type
    , current_price_type
    , current_price_amt
    , eoh_mc
    , eoh
    , asoh_mc        
    , asoh
    , mc_instock_ind
    , fc_instock_ind
    , demand
    , items
    , product_views
    , traffic
    , hist_items
    , hist_items_style
    , pct_items
    , allocated_traffic
    , CURRENT_TIMESTAMP as dw_sys_load_tmstp
  from combine_rack
  where (business_unit_desc = 'RACK' and day_date >= '2020-02-01') -- Bring in both RP/Non-RP from 2020 onward
    -- or (business_unit_desc = 'RACK CANADA' and day_date >= '2020-02-01') --Only Since FY2020
    or (business_unit_desc = 'RACK' and rp_idnt = 1 and day_date < '2020-02-01') --Only bring in RP if 2019
;

SET QUERY_BAND = NONE FOR SESSION;