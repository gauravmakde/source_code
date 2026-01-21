SET QUERY_BAND = 'App_ID=APP08176;
     DAG_ID=mothership_wbr_primary_11521_ACE_ENG;
     Task_Name=wbr_primary;'
     FOR SESSION VOLATILE;
 
/*

Table Name: t2dl_das_mothership.wbr_primary
Team/Owner: tech_ffp_analytics/Matthew Bond
Date Modified: 08/23/2023

Notes:
WBR = finance Weekly Business Review tableau dashboard: https://tableau.nordstrom.com/#/site/AS/views/WBRdash/DigitalWBR?:iid=2
creates intermediate data table to make dashboard run quickly, the primary table used to power the dashboard
*/

------------------------------------------------------------------------------
-- creating day_cal tables to assist with 53rd week realignment
create multiset volatile table wk_53_realign as
(select fiscal_year_num+1 as fiscal_year_num_ly, max(fiscal_week_num) max_wk_num_ly
from prd_nap_usr_vws.day_cal_454_dim ty
group by 1
where fiscal_year_num between 2021 and (select fiscal_year_num from prd_nap_usr_vws.day_cal_454_dim where day_date = current_date)
)with data primary index(fiscal_year_num_ly) on commit preserve rows;

-- adding ly week number columns as part of 53rd week solution
-- the below calculations ensure that week 53 comps to week 1 of same year, and for year following
-- 53 week year, we have fiscal week N compared to week N+1 of the prior year (e.g week 202403 is compared to week 202304)
create multiset volatile table fw as
(select
       distinct ty.fiscal_year_num,
       week_idnt as week_num,
       fiscal_week_num,
       max_wk_num_ly,
       case when max_wk_num_ly = 52 and fiscal_week_num < 53 then week_num - 100
            when max_wk_num_ly = 52 and fiscal_week_num = 53 then week_num - 52
            when max_wk_num_ly = 53 then week_num - 99
       end as ly_week_num
from prd_nap_usr_vws.day_cal_454_dim ty
    join wk_53_realign ly on ty.fiscal_year_num = ly.fiscal_year_num_ly
where week_num between 202101 and (select week_num from prd_nap_usr_vws.day_cal where day_date = current_date)-1
) with data primary index(week_num) on commit preserve rows;

create multiset volatile table bopus as
(
select
week_num
, case when business_unit_desc_bopus_in_store_demand = 'OFFPRICE ONLINE' then 'R.COM' else business_unit_desc_bopus_in_store_demand end as bu
, sum(gross_merch_sales_amt) as gross_sales_excl_bopus
, sum(demand_canceled_amt) as canceled_demand_excl_bopus
from T2DL_DAS_MOTHERSHIP.finance_sales_demand_fact as fsdf
LEFT JOIN PRD_NAP_USR_VWS.DAY_CAL as w
    ON fsdf.tran_date = w.day_date
where (week_num in (select week_num from fw) or week_num in (select ly_week_num from fw))
and customer_journey <> 'BOPUS'
and business_unit_desc_bopus_in_store_demand in ('N.COM', 'OFFPRICE ONLINE')
group by 1,2
) with data primary index(week_num,bu) on commit preserve rows;

create multiset volatile table sales as
(
select
  dt.week_num
, dt.year_num as fy
, case when sdotf.business_unit_desc = 'OFFPRICE ONLINE' then 'R.COM' else sdotf.business_unit_desc end as business_unit_desc
, sum(demand_amt_excl_bopus + bopus_attr_digital_amt) as demand
, sum(demand_amt_excl_bopus) as demand_excl_bopus
, sum(demand_units_excl_bopus + bopus_attr_digital_units) as units
, sum(orders) as orders
, sum(visitors) as udv
, sum(ordering_visitors) as ordering_udv
, sum(gross_merch_sales_amt) as gross_merch_sales_amt
, sum(merch_returns_amt) as merch_returns_amt
, sum(net_merch_sales_amt) as net_merch_sales_amt
from T2DL_DAS_MOTHERSHIP.SALES_DEMAND_ORDERS_TRAFFIC_FACT as sdotf
left join PRD_NAP_USR_VWS.DAY_CAL as dt on dt.day_date = sdotf.tran_date
left join PRD_NAP_USR_VWS.STORE_DIM as sd on sd.store_num = sdotf.store_num
where 1=1
and sd.business_unit_desc = sdotf.business_unit_desc --removes DTC orders that have FLS store numbers (not 808)
-- DTC should be in stores (DTC sales and demand are in stores) and is counted in purchase_trips already. This logic is copied from how finance pulls to the mothership excel
and sdotf.business_unit_desc in ('N.COM', 'OFFPRICE ONLINE') --'N.CA',
and sdotf.store_num not in (141, 173)--trunk club
and (week_num in (select week_num from fw) or week_num in (select ly_week_num from fw))
group by 1,2,3
) with data primary index(week_num,business_unit_desc) on commit preserve rows;

create multiset volatile table combo as
(
SELECT
  sales.week_num
, sales.fy
, sales.business_unit_desc as source_site
, demand
, demand_excl_bopus
, units
, canceled_demand_excl_bopus
, orders
, udv
, ordering_udv
, gross_merch_sales_amt
, bopus.gross_sales_excl_bopus
, merch_returns_amt
, net_merch_sales_amt
from sales
left join bopus
on bopus.week_num = sales.week_num and bopus.bu = sales.business_unit_desc
) with data primary index(week_num,source_site) on commit preserve rows;

create multiset volatile table ly as (
select
  fw.week_num as week_num
, source_site
, demand as demand_ly
, demand_excl_bopus as demand_excl_bopus_ly
, orders as orders_ly
, udv as udv_ly
, ordering_udv as ordering_udv_ly
, net_merch_sales_amt as net_merch_sales_amt_ly
, units as units_ly
from fw
    join combo on fw.ly_week_num = combo.week_num
where combo.week_num in (select week_num from fw) or combo.week_num in (select ly_week_num from fw)
) with data primary index(week_num,source_site) on commit preserve rows;


create multiset volatile table grow as
(
select
 ty.week_num
, ty.source_site as box
, udv, udv_ly, ordering_udv, ordering_udv_ly
, canceled_demand_excl_bopus
, units, units_ly
, demand_excl_bopus, demand_excl_bopus_ly, demand, demand_ly
, gross_sales_excl_bopus, gross_merch_sales_amt
, merch_returns_amt
, net_merch_sales_amt, net_merch_sales_amt_ly
, orders, orders_ly
from combo as ty
left join ly
on ty.week_num = ly.week_num and ty.source_site = ly.source_site
where ty.week_num in (select week_num from fw)
) with data primary index(week_num,box) on commit preserve rows;


create multiset volatile table car as 
(--cust acquisition (ntn) and retention
select a.report_period as week_num,
    case when box = 'NCOM' then 'N.COM' else 'R.COM' end as box,
    sum(new_customer) new_customer,
    sum(retained_customer) retained_customer,
    sum(react_customer) react_customer,
    sum(total_customer) total_customer,
    sum(total_trips) total_trips
from dl_cma_cmbr.week_num_buyerflow_segment a
where box in ('NCOM','NRHL')
    and country <> 'CA'
and (report_period in (select week_num from fw))
group by 1,2
) with data primary index(week_num,box) on commit preserve rows;


create multiset volatile table nsr_prep as
(SELECT week_num,
       Nordstrom_nsr,
       Nordstrom_Rack_nsr
FROM (SELECT week_num,
             (SUM(CASE WHEN order_channel IN ('NCOM','FLS') THEN attributed_pred_net ELSE 0 END)) /(nullifzero (SUM(CASE WHEN arrived_channel IN ('NCOM') AND funnel_type = 'low' THEN cost ELSE 0 END))) AS Nordstrom_nsr,
             (SUM(CASE WHEN order_channel IN ('RCOM','RACK') THEN attributed_pred_net ELSE 0 END)) /(nullifzero (SUM(CASE WHEN arrived_channel IN ('RCOM') AND funnel_type = 'low' THEN cost ELSE 0 END))) AS Nordstrom_Rack_nsr
      FROM T2DL_DAS_MTA.mta_ssa_cost_agg_vw AS mta
        LEFT JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM AS w ON mta.activity_date_pacific = w.day_date
      WHERE (week_num IN (SELECT week_num FROM fw))
      AND   channelcountry = 'US'
      AND   marketing_type = 'paid'
      AND   (arrived_channel IN ('NCOM','RCOM') OR (arrived_channel = 'NULL' AND order_channel IN ('FLS','NCOM','RACK','RCOM')))
      GROUP BY 1) AS a) WITH data PRIMARY INDEX (week_num) ON COMMIT preserve rows;


create multiset volatile table nsr as
(select week_num,
       'N.COM' as box,
       nordstrom_nsr as nsr
from nsr_prep
UNION
select week_num,
       'R.COM' as box,
       nordstrom_rack_nsr as nsr
from nsr_prep
) with data primary index(week_num,box) on commit preserve rows;


CREATE multiset volatile TABLE nsr_plan_prep
AS
(SELECT week_num,
       Nordstrom_nsr_plan,
       Nordstrom_Rack_nsr_plan
FROM (SELECT week_num,
             (SUM(CASE WHEN order_channel IN ('NCOM','FLS') THEN fcst_attributed_pred_net ELSE 0 END)) /(nullifzero (SUM(CASE WHEN order_channel IN ('NCOM')  THEN fcst_cost ELSE 0 END))) AS Nordstrom_nsr_plan,
             (SUM(CASE WHEN order_channel IN ('RCOM','RACK') THEN fcst_attributed_pred_net ELSE 0 END)) /(nullifzero (SUM(CASE WHEN order_channel IN ('RCOM') THEN fcst_cost ELSE 0 END))) AS Nordstrom_Rack_nsr_plan
      FROM T2DL_DAS_MTA.mta_ssa_cost_fcst_agg_vw AS mta
        LEFT JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM AS w ON mta.activity_date_pacific = w.day_date
      WHERE (week_num IN (SELECT week_num FROM fw))
      AND   channelcountry = 'US'
      AND   marketing_type = 'paid'
      AND   (arrived_channel IN ('NCOM','RCOM','RACK') OR (arrived_channel = 'NULL' AND order_channel IN ('FLS','NCOM','RACK','RCOM')))
      GROUP BY 1) AS a) WITH data PRIMARY INDEX (week_num) ON COMMIT preserve rows;

create multiset volatile table nsr_plan as
(select week_num,
       'N.COM' as box,
       nordstrom_nsr_plan as nsr_plan
from nsr_plan_prep
UNION
select week_num,
       'R.COM' as box,
       nordstrom_rack_nsr_plan as nsr_plan
from nsr_plan_prep) with data primary index(week_num,box) on commit preserve rows;


create multiset volatile table twist as
(
select
    week_idnt week_num
        , case when channel = 'FULL_LINE' then 'N.COM' else 'R.COM' end as box
        , sum (pf.product_views*pf.pct_instock)/ sum (case when pf.pct_instock is not null then pf.product_views end) as twist
from t2dl_das_product_funnel.product_price_funnel_daily pf
    inner join PRD_NAP_USR_VWS.day_cal_454_dim dc
on dc.day_date = pf.event_date_pacific
where pf.channelcountry in ('US')
  and dc.week_idnt in (select week_num from fw)
  and current_price_type = 'R'
group by 1, 2
) with data primary index(week_num,box) on commit preserve rows;


create multiset volatile table sff as
(
select x1.week_num week_num,
       x1.box box,
       x3.sff_demand_units_bopus_in_digital units_sff_shipped,
       x2.units_ds_shipped units_ds_shipped,
       x1.total_demand_units_bopus_in_digital units_shipped,
       1.0000*units_sff_shipped/units_shipped as pct_sff_units_shipped,
       1.0000*units_ds_shipped/units_shipped as pct_ds_units_shipped
from
(select
    week_idnt week_num
    ,  case when business_unit_desc_bopus_in_digital_demand = 'OFFPRICE ONLINE' then 'R.COM'
       else business_unit_desc_bopus_in_digital_demand
       end as box
    , sum(demand_units_bopus_in_digital) as total_demand_units_bopus_in_digital
from T2DL_DAS_MOTHERSHIP.finance_sales_demand_fact fsdf
left join PRD_NAP_USR_VWS.day_cal_454_dim dc
    on dc.day_date = fsdf.tran_date
where box in ('N.COM', 'R.COM')
group by 1,2) x1
left join
(select
      week_idnt week_num
    , case when business_unit_desc_bopus_in_digital_demand = 'OFFPRICE ONLINE' then 'R.COM'
       else business_unit_desc_bopus_in_digital_demand
       end as box
    , sum(demand_units_bopus_in_digital) as units_ds_shipped
from T2DL_DAS_MOTHERSHIP.finance_sales_demand_fact fsdf
left join PRD_NAP_USR_VWS.day_cal_454_dim dc
    on dc.day_date = fsdf.tran_date
where customer_journey in ('ShipToStore_from_DropShip','ShipToHome_from_DropShip')
    and box in ('N.COM', 'R.COM')
    group by 1,2) x2
on x1.week_num = x2.week_num
    and x1.box = x2.box
left join
(select
    week_idnt week_num
    ,  case when business_unit_desc_bopus_in_digital_demand = 'OFFPRICE ONLINE' then 'R.COM'
       else business_unit_desc_bopus_in_digital_demand
       end as box
    , sum(demand_units_bopus_in_digital) as sff_demand_units_bopus_in_digital
from T2DL_DAS_MOTHERSHIP.finance_sales_demand_fact fsdf
left join PRD_NAP_USR_VWS.day_cal_454_dim dc
    on dc.day_date = fsdf.tran_date
where customer_journey in ('ShipToStore_from_StoreFulfill','ShipToHome_from_StoreFulfill')
    and box in ('N.COM', 'R.COM')
group by 1,2) x3
on x1.week_num = x3.week_num
    and x1.box = x3.box
where 1=1
    and x1.box in ('N.COM', 'R.COM')
    and (x1.week_num in (select week_num from fw))
) with data primary index(week_num,box) on commit preserve rows;



create multiset volatile table p90 as
(--sushanth has an explanation for why it's slightly different than the old dashboard- this data needs an update but it's good enough.
-- changed the data source from weekly to daily to align with P90 dashboard
select
case when business_unit = 'NCOM' then 'N.COM' else 'R.COM' end as box
, concat(cast(cycle_year as varchar(4)),right(cast(CYCLE_WEEK_OF_FISCAL_YEAR+100 as varchar(3)),2)) as week_num
, AVG(p90) as P90
from T2DL_SCA_VWS.PACKAGE_P50P90_DAILY_NODE_VW
where KPI = 'Click_to_deliver'
and (week_num in (select week_num from fw))
and node_type = 'all_up'
and node = 'all_up'
and KPI_COHORT = 3
group by 1,2
) with data primary index(week_num,box) on commit preserve rows;


create multiset volatile table backlog as
(SELECT
    Fiscal_Year*100 + Fiscal_Week as week_num
   ,case when ss_Business_Unit_21_Day_Labor = 'NRHL' then 'R.COM' else 'N.COM' end as box
   , sum(actual_backlog) as fc_backlog
FROM T2DL_SCA_VWS.DAILY_OUTBOUND_FCT_VW
where ss_Business_Unit_21_Day_Labor in ('NCOM', 'NRHL')
and week_num in (select week_num from fw)
and DOW = 'Sat'
group by 1,2
) with data primary index(week_num,box) on commit preserve rows;


create multiset volatile table wbr_summary_table as
(select
  grow.week_num as fw
, grow.box
, car.new_customer
, car.retained_customer
, car.react_customer
, car.total_customer
, car.total_trips
, nsr
, twist
, sff.units_shipped
, sff.units_sff_shipped
, sff.units_ds_shipped
, P90
, fc_backlog
, lag (new_customer,1,null) over (partition by car.box order by car.week_num) as new_customer_lag
, lag (retained_customer,1,null) over (partition by car.box order by car.week_num) as retained_customer_lag
, lag (react_customer,1,null) over (partition by car.box order by car.week_num) as react_customer_lag
, lag (total_customer,1,null) over (partition by car.box order by car.week_num) as total_customer_lag
, lag (total_trips,1,null) over (partition by car.box order by car.week_num) as total_trips_lag
, lag (nsr,1,null) over (partition by nsr.box order by nsr.week_num) as nsr_lag
, lag (twist,1,null) over (partition by twist.box order by twist.week_num) as twist_lag
, lag (units_shipped,1,null) over (partition by sff.box order by sff.week_num) as units_shipped_lag
, lag (units_sff_shipped,1,null) over (partition by sff.box order by sff.week_num) as units_sff_shipped_lag
, lag (units_ds_shipped,1,null) over (partition by sff.box order by sff.week_num) as units_ds_shipped_lag
, lag (P90,1,null) over (partition by p90.box order by p90.week_num) as p90_lag
, lag (fc_backlog,1,null) over (partition by backlog.box order by backlog.week_num) as fc_backlog_lag
, udv, udv_ly, ordering_udv, ordering_udv_ly
, units, units_ly
, demand_excl_bopus, demand, demand_ly, demand_excl_bopus_ly
, gross_sales_excl_bopus, gross_merch_sales_amt
, merch_returns_amt, net_merch_sales_amt, net_merch_sales_amt_ly
, orders, orders_ly
, lag (udv,1,null) over (partition by grow.box order by grow.week_num) as udv_lag
, lag (udv_ly,1,null) over (partition by grow.box order by grow.week_num) as udv_ly_lag
, lag (ordering_udv,1,null) over (partition by grow.box order by grow.week_num) as ordering_udv_lag
, lag (ordering_udv_ly,1,null) over (partition by grow.box order by grow.week_num) as ordering_udv_ly_lag
, lag (canceled_demand_excl_bopus,1,null) over (partition by grow.box order by grow.week_num) as canceled_demand_excl_bopus_lag
, lag (canceled_demand_excl_bopus,2,null) over (partition by grow.box order by grow.week_num) as canceled_demand_excl_bopus_lag2 --for cancel rate calculation, which is lagged 1 week
, lag (units,1,null) over (partition by grow.box order by grow.week_num) as units_lag
, lag (units_ly,1,null) over (partition by grow.box order by grow.week_num) as units_ly_lag
, lag (demand_excl_bopus,1,null) over (partition by grow.box order by grow.week_num) as demand_excl_bopus_lag
, lag (demand_excl_bopus,2,null) over (partition by grow.box order by grow.week_num) as demand_excl_bopus_lag2 --for cancel rate calculation, which is lagged 1 week
, lag (demand_excl_bopus_ly,1,null) over (partition by grow.box order by grow.week_num) as demand_excl_bopus_ly_lag
, lag (demand,1,null) over (partition by grow.box order by grow.week_num) as demand_lag
, lag (demand_ly,1,null) over (partition by grow.box order by grow.week_num) as demand_ly_lag
, lag (gross_sales_excl_bopus,1,null) over (partition by grow.box order by grow.week_num) as gross_sales_excl_bopus_lag
, lag (gross_merch_sales_amt,1,null) over (partition by grow.box order by grow.week_num) as gross_merch_sales_amt_lag
, lag (merch_returns_amt,1,null) over (partition by grow.box order by grow.week_num) as merch_returns_amt_lag
, lag (net_merch_sales_amt,1,null) over (partition by grow.box order by grow.week_num) as net_merch_sales_amt_lag
, lag (net_merch_sales_amt_ly,1,null) over (partition by grow.box order by grow.week_num) as net_merch_sales_amt_ly_lag
, lag (orders,1,null) over (partition by grow.box order by grow.week_num) as orders_lag
, lag (orders_ly,1,null) over (partition by grow.box order by grow.week_num) as orders_ly_lag
, nsr_plan
from grow
left join car
on grow.week_num = car.week_num and grow.box = car.box
left join nsr
on grow.week_num = nsr.week_num and grow.box = nsr.box
left join nsr_plan
on grow.week_num = nsr_plan.week_num and grow.box = nsr_plan.box
left join twist
on grow.week_num = twist.week_num and grow.box = twist.box
left join sff
on grow.week_num = sff.week_num and grow.box = sff.box
left join p90
on grow.week_num = p90.week_num and grow.box = p90.box
left join backlog
on grow.week_num = backlog.week_num and grow.box = backlog.box
) with data primary index(fw,box) on commit preserve rows;


--turn the data from vertical to horizontal to make easier for DDL and save engineering resources
create multiset volatile table pre_insert as (
SELECT fw, box, feature_name, feature_value
from wbr_summary_table UNPIVOT(feature_value FOR feature_name IN (
    new_customer, retained_customer, react_customer, total_customer, total_trips
    , nsr, twist, P90
    , units_shipped, units_sff_shipped, units_ds_shipped
    , fc_backlog
    , new_customer_lag, retained_customer_lag, react_customer_lag, total_customer_lag, total_trips_lag
    , nsr_lag, twist_lag, p90_lag
    , units_shipped_lag, units_sff_shipped_lag, units_ds_shipped_lag
    , fc_backlog_lag
    , udv, udv_ly, ordering_udv, ordering_udv_ly
    , units, units_ly
    , demand_excl_bopus, demand, demand_ly, demand_excl_bopus_ly
    , gross_sales_excl_bopus, gross_merch_sales_amt
    , merch_returns_amt, net_merch_sales_amt, net_merch_sales_amt_ly
    , orders, orders_ly
    , udv_lag, udv_ly_lag, ordering_udv_lag, ordering_udv_ly_lag
    , canceled_demand_excl_bopus_lag, canceled_demand_excl_bopus_lag2
    , units_lag, units_ly_lag
    , demand_excl_bopus_lag, demand_excl_bopus_lag2, demand_excl_bopus_ly_lag
    , demand_lag, demand_ly_lag
    , gross_sales_excl_bopus_lag, gross_merch_sales_amt_lag, merch_returns_amt_lag
    , net_merch_sales_amt_lag, net_merch_sales_amt_ly_lag
    , orders_lag, orders_ly_lag
    , nsr_plan
)) as temp_pivot
) with data primary index(fw,box) on commit preserve rows;



DELETE FROM {mothership_t2_schema}.wbr_primary;


--------------------------------------------------------------------
/*
final table creation
*/
--T2DL_DAS_MOTHERSHIP.wbr_primary

INSERT INTO {mothership_t2_schema}.wbr_primary
SELECT fw as week_num, box as business_unit_desc, feature_name, feature_value, CURRENT_TIMESTAMP as dw_sys_load_tmstp
from pre_insert
 ;

COLLECT STATISTICS COLUMN(week_num), COLUMN(business_unit_desc), COLUMN(feature_name) ON {mothership_t2_schema}.wbr_primary;


SET QUERY_BAND = NONE FOR SESSION;